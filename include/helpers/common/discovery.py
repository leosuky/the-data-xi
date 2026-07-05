"""
include/helpers/common/discovery.py
====================================
Scans the fixtures directory for match data organised in provider subdirectories.

Expected directory structure:
    {fixtures_dir}/
    └── {combo_id}/
        ├── whoscored/    → whoscored_{ws_id}.json    (required)
        ├── fotmob/       → fotmob_{fm_id}.json       (optional)
        ├── sofascore/    → {sf_id}_{type}.json        (optional)
        └── oddspedia/    → {op_id}_odds_{market}.json (optional)

A match folder is valid if it has a whoscored/ subdirectory with at least
one whoscored_*.json file.

Discovery is split into two concerns:
    _scan_fixtures()        — pure filesystem scan → match descriptors
    _get_loaded_combo_ids() — DB diff (what's already in RAW)

discover_unloaded_matches() composes them. With force_reload=True the DB diff
is skipped entirely (the DB is never queried), so every valid match on disk is
returned for reprocessing — used after a parser or schema change.
"""

import os
import re
import json
import logging

log = logging.getLogger(__name__)


def is_valid_ws_matchcentre(data) -> bool:
    """
    True only for REAL WhoScored matchCentreData.

    When a match has no centre data, WhoScored serves a stub — a flat
    {eventType: index} lookup dictionary — which is valid JSON but has none of
    the match structure the parsers need. A genuine feed has home/away team
    objects (with teamId) and an events list. This distinguishes the two so the
    pipeline can skip WhoScored for that match instead of crashing on it.
    """
    return (
        isinstance(data, dict)
        and isinstance(data.get('home'), dict)
        and isinstance(data.get('away'), dict)
        and 'teamId' in data['home']
        and isinstance(data.get('events'), list)
    )


def _get_loaded_combo_ids(conn, schema: str) -> set[str]:
    """Query ws_match_meta to find already-loaded combo_ids."""
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT combo_id FROM {schema}.ws_match_meta')
        loaded = {row[0] for row in cur.fetchall()}
        cur.close()
        return loaded
    except Exception as e:
        log.warning(f'Could not query loaded matches (table may not exist): {e}')
        conn.rollback()
        return set()


def _scan_fixtures(fixtures_dir: str) -> list[dict]:
    """
    Pure filesystem scan: walk the fixtures tree and build a descriptor for
    every valid match folder (one with a whoscored/whoscored_*.json). No DB
    access. Returns descriptors sorted by combo_id.
    """
    discovered = []

    for combo_id in sorted(os.listdir(fixtures_dir)):
        combo_path = os.path.join(fixtures_dir, combo_id)
        if not os.path.isdir(combo_path):
            continue

        # WhoScored subdirectory + JSON is required
        ws_dir = os.path.join(combo_path, 'whoscored')
        if not os.path.isdir(ws_dir):
            continue

        ws_path = ws_id = None
        for f in os.listdir(ws_dir):
            m = re.match(r'^whoscored_(\d+)\.json$', f)
            if m:
                ws_path = os.path.join(ws_dir, f)
                ws_id = int(m.group(1))
                break
        if not ws_path:
            log.warning(f'No whoscored JSON in {ws_dir}, skipping')
            continue

        # Validate the WhoScored CONTENT (not just the filename). A match with a
        # stub/garbage centre-data file is treated as WhoScored-absent.
        ws_valid = False
        try:
            with open(ws_path, 'r', encoding='utf-8') as fp:
                ws_valid = is_valid_ws_matchcentre(json.load(fp))
        except Exception as e:
            log.warning(f'{ws_path}: unreadable WhoScored JSON ({e})')

        # Optional provider subdirectories
        fm_dir = os.path.join(combo_path, 'fotmob')
        sf_dir = os.path.join(combo_path, 'sofascore')
        odds_dir = os.path.join(combo_path, 'oddspedia')

        fm_path = fm_id = None
        if os.path.isdir(fm_dir):
            for f in os.listdir(fm_dir):
                m = re.match(r'^fotmob_(\d+)\.json$', f)
                if m:
                    fm_path = os.path.join(fm_dir, f)
                    fm_id = int(m.group(1))
                    break

        has_other = (fm_path is not None
                     or os.path.isdir(sf_dir) or os.path.isdir(odds_dir))

        # A WhoScored-invalid match is still loadable from the other providers
        # (identity resolves via the global crosswalk). Only skip if there is
        # genuinely nothing to load.
        if not ws_valid:
            if not has_other:
                log.warning(f'{combo_id}: WhoScored invalid and no other '
                            f'providers present — skipping')
                continue
            log.warning(f'{combo_id}: WhoScored matchCentreData missing/invalid — '
                        f'loading other providers only (identity via crosswalk; '
                        f'WhoScored-derived facts will be empty for this match)')

        discovered.append({
            'combo_id':             combo_id,
            'match_dir':            combo_path,
            'ws_path':              ws_path,
            'ws_id':                ws_id,
            'ws_valid':             ws_valid,
            'fm_path':              fm_path,
            'fm_id':                fm_id,
            'sf_dir':               sf_dir if os.path.isdir(sf_dir) else None,
            'odds_dir':             odds_dir if os.path.isdir(odds_dir) else None,
            'has_ws_events':        ws_valid,
            'has_fotmob_data':      fm_path is not None,
            'has_sofascore_data':   os.path.isdir(sf_dir),
            'has_odds_data':        os.path.isdir(odds_dir),
        })

    return discovered


def discover_unloaded_matches(
    fixtures_dir: str,
    conn,
    schema: str = 'raw',
    force_reload: bool = False,
) -> list[dict]:
    """
    Scan the fixtures directory and return match descriptors for Airflow task
    mapping.

    Args:
        fixtures_dir: Root path containing combo_id directories.
        conn:         psycopg2 connection (unused when force_reload=True).
        schema:       Postgres schema (default: 'raw').
        force_reload: If True, bypass the DB diff and return ALL valid matches
                      on disk (reprocess everything). The loader is idempotent.

    Returns:
        List of match descriptor dicts.
    """
    if not os.path.isdir(fixtures_dir):
        log.error(f'Fixtures directory not found: {fixtures_dir}')
        return []

    all_matches = _scan_fixtures(fixtures_dir)
    log.info(f'{len(all_matches)} valid match folders on disk')

    if force_reload:
        selected = all_matches
        log.info(f'force_reload=True → reprocessing all {len(selected)} matches '
                 f'(DB diff skipped)')
    else:
        loaded_ids = _get_loaded_combo_ids(conn, schema)
        log.info(f'{len(loaded_ids)} already loaded in {schema}.ws_match_meta')
        selected = [m for m in all_matches if m['combo_id'] not in loaded_ids]

    log.info(
        f'Selected {len(selected)} matches to load '
        f'({sum(1 for d in selected if d["has_fotmob_data"])} with Fotmob, '
        f'{sum(1 for d in selected if d["has_sofascore_data"])} with Sofascore, '
        f'{sum(1 for d in selected if d["has_odds_data"])} with odds)'
    )

    return selected