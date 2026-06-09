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
"""

import os
import re
import logging

log = logging.getLogger(__name__)


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


def discover_unloaded_matches(
    fixtures_dir: str,
    conn,
    schema: str = 'raw',
) -> list[dict]:
    """
    Scan fixtures directory for match folders with provider subdirectories.
    Diff against the database to find matches not yet loaded.

    Args:
        fixtures_dir: Root path containing combo_id directories.
        conn:         psycopg2 connection.
        schema:       Postgres schema (default: 'raw').

    Returns:
        List of match descriptor dicts for Airflow task mapping.
    """
    if not os.path.isdir(fixtures_dir):
        log.error(f'Fixtures directory not found: {fixtures_dir}')
        return []

    loaded_ids = _get_loaded_combo_ids(conn, schema)
    log.info(f'{len(loaded_ids)} matches already loaded in {schema}.ws_match_meta')

    discovered = []

    # Each immediate subdirectory of fixtures_dir is a combo_id
    for combo_id in sorted(os.listdir(fixtures_dir)):
        combo_path = os.path.join(fixtures_dir, combo_id)
        if not os.path.isdir(combo_path):
            continue

        # Skip already loaded
        if combo_id in loaded_ids:
            continue

        # Check for whoscored/ subdirectory (required)
        ws_dir = os.path.join(combo_path, 'whoscored')
        if not os.path.isdir(ws_dir):
            continue

        # Find the WhoScored JSON
        ws_path = None
        ws_id = None
        for f in os.listdir(ws_dir):
            m = re.match(r'^whoscored_(\d+)\.json$', f)
            if m:
                ws_path = os.path.join(ws_dir, f)
                ws_id = int(m.group(1))
                break

        if not ws_path:
            log.warning(f'No whoscored JSON in {ws_dir}, skipping')
            continue

        # Check optional provider subdirectories
        fm_dir = os.path.join(combo_path, 'fotmob')
        sf_dir = os.path.join(combo_path, 'sofascore')
        odds_dir = os.path.join(combo_path, 'oddspedia')

        # Find Fotmob file
        fm_path = None
        fm_id = None
        if os.path.isdir(fm_dir):
            for f in os.listdir(fm_dir):
                m = re.match(r'^fotmob_(\d+)\.json$', f)
                if m:
                    fm_path = os.path.join(fm_dir, f)
                    fm_id = int(m.group(1))
                    break

        descriptor = {
            'combo_id':             combo_id,
            'match_dir':            combo_path,
            'ws_path':              ws_path,
            'ws_id':                ws_id,
            'fm_path':              fm_path,
            'fm_id':                fm_id,
            'sf_dir':               sf_dir if os.path.isdir(sf_dir) else None,
            'odds_dir':             odds_dir if os.path.isdir(odds_dir) else None,
            'has_ws_events':        True,
            'has_fotmob_data':      fm_path is not None,
            'has_sofascore_data':   os.path.isdir(sf_dir),
            'has_odds_data':        os.path.isdir(odds_dir),
        }

        discovered.append(descriptor)

    log.info(
        f'Discovered {len(discovered)} unloaded matches '
        f'({sum(1 for d in discovered if d["has_fotmob_data"])} with Fotmob, '
        f'{sum(1 for d in discovered if d["has_sofascore_data"])} with Sofascore, '
        f'{sum(1 for d in discovered if d["has_odds_data"])} with odds)'
    )

    return discovered