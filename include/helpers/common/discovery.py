"""
include/helpers/common/discovery.py
====================================
Scans the raw-data tree for match data organised in provider subdirectories.

Expected directory structure (mirrors the scraper's `Done` folder):
    {data_root}/
    └── {competition}/                 e.g. Premier_League
        └── {season}/                  e.g. 2026_2027
            └── {combo_id}/
                ├── whoscored/    → whoscored_{ws_id}.json    (optional*)
                ├── fotmob/       → fotmob_{fm_id}.json       (optional)
                ├── sofascore/    → {sf_id}_{type}.json        (optional)
                └── oddspedia/    → {op_id}_odds_{market}.json (optional)

`fixtures_dir` may point at the data root (every competition/season is
scanned, optionally filtered) OR directly at a single season folder (its
immediate subdirectories are combo_ids) — both are detected automatically.

*A match is loadable if it has valid WhoScored matchCentreData OR at least one
other provider. WhoScored-absent matches (missing folder, missing file, or a
stub file — e.g. the whole Saudi Pro League) load the other providers only.

Discovery is split into two concerns:
    _scan_fixtures()        — pure filesystem scan of ONE season folder
    _get_loaded_combo_ids() — DB diff (what's already in RAW)

discover_unloaded_matches() composes them across every selected season
folder. With force_reload=True the DB diff is skipped entirely (the DB is
never queried), so every valid match on disk is returned for reprocessing —
used after a parser or schema change.
"""

import os
import re
import json
import logging

log = logging.getLogger(__name__)

# combo_id folder: match date + team codes, e.g. 2026-08-21ArsCov
COMBO_DIR_RE = re.compile(r'^\d{4}-\d{2}-\d{2}\S+$')


def parse_csv_param(value) -> list[str] | None:
    """'Premier_League, UCL' / ['UCL'] / '' / None -> ['Premier_League','UCL'] / ['UCL'] / None."""
    if not value:
        return None
    items = value if isinstance(value, (list, tuple)) else str(value).split(',')
    items = [str(i).strip() for i in items if str(i).strip()]
    return items or None


def _is_season_dir(path: str) -> bool:
    """True if `path`'s immediate subdirectories are combo_id folders."""
    try:
        return any(COMBO_DIR_RE.match(d) and os.path.isdir(os.path.join(path, d))
                   for d in os.listdir(path))
    except OSError:
        return False


def find_season_dirs(
    root: str,
    competitions: list[str] | None = None,
    seasons: list[str] | None = None,
) -> list[tuple[str | None, str | None, str]]:
    """
    Resolve `root` to the season folders to scan, as (competition, season, path).

    `root` is either a single season folder (returned as-is; filters ignored)
    or the data root laid out as {competition}/{season}/.
    """
    if _is_season_dir(root):
        return [(None, None, root)]

    out = []
    for comp in sorted(os.listdir(root)):
        comp_path = os.path.join(root, comp)
        if not os.path.isdir(comp_path):
            continue
        if competitions and comp not in competitions:
            continue
        for season in sorted(os.listdir(comp_path)):
            season_path = os.path.join(comp_path, season)
            if not os.path.isdir(season_path):
                continue
            if seasons and season not in seasons:
                continue
            out.append((comp, season, season_path))
    return out


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


# Sofascore status types that mean "no real match took place".
_NOT_PLAYED_STATUS = {'canceled', 'postponed'}


def _sofa_status(sf_dir: str | None) -> tuple[str | None, str | None]:
    """(status type, description) from the Sofascore main event file, or (None, None)."""
    if not sf_dir or not os.path.isdir(sf_dir):
        return None, None
    for f in os.listdir(sf_dir):
        if f.endswith('_main_event_data.json'):
            try:
                with open(os.path.join(sf_dir, f), 'r', encoding='utf-8') as fp:
                    status = (json.load(fp).get('event') or {}).get('status') or {}
                return status.get('type'), status.get('description')
            except Exception:
                return None, None
    return None, None


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


def _scan_fixtures(fixtures_dir: str, competition: str | None = None,
                   season: str | None = None,
                   skip_ids: set[str] | None = None) -> list[dict]:
    """
    Pure filesystem scan of ONE season folder: build a descriptor for every
    loadable match folder (valid WhoScored and/or any other provider). No DB
    access. Returns descriptors sorted by combo_id.

    skip_ids: combo_ids to skip WITHOUT opening any of their files (the
    already-loaded matches). Validating a match means parsing its ~2.5 MB
    WhoScored JSON, so scanning only the unloaded folders keeps discovery
    proportional to new data instead of everything ever loaded.
    """
    discovered = []
    skip_ids = skip_ids or set()

    for combo_id in sorted(os.listdir(fixtures_dir)):
        if combo_id in skip_ids:
            continue
        combo_path = os.path.join(fixtures_dir, combo_id)
        if not os.path.isdir(combo_path):
            continue

        # WhoScored is optional: a missing folder, missing file, or stub file
        # all mean "WhoScored-absent" (handled below), not "skip the match".
        ws_dir = os.path.join(combo_path, 'whoscored')
        ws_path = ws_id = None
        if os.path.isdir(ws_dir):
            for f in os.listdir(ws_dir):
                m = re.match(r'^whoscored_(\d+)\.json$', f)
                if m:
                    ws_path = os.path.join(ws_dir, f)
                    ws_id = int(m.group(1))
                    break

        # Validate the WhoScored CONTENT (not just the filename). A match with a
        # stub/garbage centre-data file is treated as WhoScored-absent.
        ws_valid = False
        if ws_path:
            try:
                with open(ws_path, 'r', encoding='utf-8') as fp:
                    ws_valid = is_valid_ws_matchcentre(json.load(fp))
            except Exception as e:
                log.warning(f'{ws_path}: unreadable WhoScored JSON ({e})')

        # Optional provider subdirectories
        fm_dir = os.path.join(combo_path, 'fotmob')
        sf_dir = os.path.join(combo_path, 'sofascore')
        odds_dir = os.path.join(combo_path, 'oddspedia')

        # Cancelled / abandoned / postponed matches carry no real match data
        # (FotMob still ships a "finished 0-0" shell) — never load them.
        st_type, st_desc = _sofa_status(sf_dir)
        if st_type in _NOT_PLAYED_STATUS:
            log.warning(f'{combo_id}: Sofascore status {st_type!r} ({st_desc}) — '
                        f'not a played match, skipping')
            continue

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
            # Per-match detail at DEBUG: whole leagues (Saudi) are WhoScored-
            # absent by design; the load DAG logs the summary list.
            log.debug(f'{combo_id}: WhoScored matchCentreData missing/invalid — '
                      f'loading other providers only (identity via crosswalk; '
                      f'WhoScored-derived facts will be empty for this match)')

        discovered.append({
            'combo_id':             combo_id,
            'competition':          competition,
            'season':               season,
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
    competitions: list[str] | None = None,
    seasons: list[str] | None = None,
    combo_ids: list[str] | None = None,
) -> list[dict]:
    """
    Scan the raw-data tree and return match descriptors for Airflow task
    mapping.

    Args:
        fixtures_dir: The data root ({competition}/{season}/{combo_id}) or a
                      single season folder ({combo_id}/ directly beneath).
        conn:         psycopg2 connection (unused when force_reload=True).
        schema:       Postgres schema (default: 'raw').
        force_reload: If True, bypass the DB diff and return ALL valid matches
                      on disk (reprocess everything). The loader is idempotent.
        competitions: Only these competition folders (None = all).
        seasons:      Only these season folders (None = all).
        combo_ids:    Only these matches (None = all).

    Returns:
        List of match descriptor dicts.
    """
    if not os.path.isdir(fixtures_dir):
        log.error(f'Fixtures directory not found: {fixtures_dir}')
        return []

    season_dirs = find_season_dirs(fixtures_dir, competitions, seasons)
    log.info(f'Scanning {len(season_dirs)} season folder(s) under {fixtures_dir}')

    # DB diff FIRST, so already-loaded folders are never opened (see _scan_fixtures).
    loaded_ids = set() if force_reload else _get_loaded_combo_ids(conn, schema)
    if not force_reload:
        log.info(f'{len(loaded_ids)} already loaded in {schema}.ws_match_meta '
                 f'(their folders are skipped unopened)')

    all_matches, seen = [], {}
    for comp, season, path in season_dirs:
        found = _scan_fixtures(path, comp, season, skip_ids=loaded_ids)
        log.info(f'  {comp or "-"}/{season or os.path.basename(path)}: '
                 f'{len(found)} match folders')
        for m in found:
            # combo_id is the global match key; the same id in two folders
            # would load twice into the same rows — keep the first, flag it.
            if m['combo_id'] in seen:
                log.warning(f'Duplicate combo_id {m["combo_id"]} in {m["match_dir"]} '
                            f'(already found in {seen[m["combo_id"]]}) — skipping')
                continue
            seen[m['combo_id']] = m['match_dir']
            all_matches.append(m)

    if combo_ids:
        wanted = set(combo_ids)
        all_matches = [m for m in all_matches if m['combo_id'] in wanted]
        missing = wanted - {m['combo_id'] for m in all_matches}
        if missing:
            log.warning(f'combo_ids not found on disk: {sorted(missing)}')

    selected = all_matches
    if force_reload:
        log.info(f'force_reload=True → reprocessing all {len(selected)} valid '
                 f'match folders on disk (DB diff skipped)')
    else:
        log.info(f'{len(selected)} valid unloaded match folders on disk')

    log.info(
        f'Selected {len(selected)} matches to load '
        f'({sum(1 for d in selected if d["has_fotmob_data"])} with Fotmob, '
        f'{sum(1 for d in selected if d["has_sofascore_data"])} with Sofascore, '
        f'{sum(1 for d in selected if d["has_odds_data"])} with odds)'
    )

    return selected
