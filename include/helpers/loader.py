"""
include/helpers/loader.py
=========================
Loads parsed match data into the RAW schema.

For each provider, calls the appropriate parser(s) and pushes results
to Postgres using upsert with column introspection (unknown columns
are silently stripped).

Tables with natural keys (combo_id + team_id/player_id) use ON CONFLICT
DO UPDATE for idempotent re-runs.

Event tables without natural keys (ws_sequences, ws_pass_map, fot_shots)
use DELETE + INSERT per combo_id.

Usage:
    from include.helpers.loader import load_match
    load_match(match_descriptor, conn, schema='raw')
"""

import os
import re
import json
import logging

from include.helpers.common.db import (
    upsert, upsert_many, remap_row, delete_match_rows,
)

log = logging.getLogger(__name__)

# ── File I/O ──────────────────────────────────────────────────────────────────

def _load_json(path):
    """Load JSON, returning None if missing or invalid."""
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict) and data.get('code') == 'TURNSTILE_REQUIRED':
            return None
        return data
    except (json.JSONDecodeError, OSError):
        return None


def _extract_id_from_filename(path, pattern):
    """Extract numeric ID from filename using regex."""
    if not path:
        return None
    m = re.search(pattern, os.path.basename(path))
    return int(m.group(1)) if m else None


# ── Generic table pusher ──────────────────────────────────────────────────────

def _push_table(cur, table, rows, conflict_cols, schema, combo_id,
                delete_first=False):
    """
    Push a list of row dicts to a RAW table.
    Wrapped in a SAVEPOINT so a single table failure doesn't
    abort the entire provider transaction.
    """
    if not rows:
        return 0

    cur.execute(f'SAVEPOINT before_{table}')
    try:
        if delete_first and combo_id:
            cur.execute(
                f"DELETE FROM {schema}.{table} WHERE combo_id = %s",
                (combo_id,)
            )

        for row in rows:
            upsert(cur, table, row, conflict_cols, schema)

        cur.execute(f'RELEASE SAVEPOINT before_{table}')
        return len(rows)
    except Exception as e:
        cur.execute(f'ROLLBACK TO SAVEPOINT before_{table}')
        log.error(f'  {table} push failed: {e}')
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# SOFASCORE RESHAPE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# read_json_data.py emits some tables WIDE; the RAW schema keeps them LONG
# (better for analytics). These helpers reshape wide DataFrames into long rows.

_MS_STAT_SUFFIXES = ('_homevalue', '_awayvalue', '_hometotal', '_awaytotal')
_MS_SUFFIX_SLOT = {
    '_homevalue': 'home_value', '_awayvalue': 'away_value',
    '_hometotal': 'home_total', '_awaytotal': 'away_total',
}
_MS_SKIP = {'period', 'match_id', 'combo_id', 'row_id', 'ingested_at'}


def _melt_match_stats_long(df, combo_id):
    """
    Reshape wide match-stats (one column per stat×side×period) into long rows:
    one row per (period, stat_name) with home/away value+total.

    Stat names are recovered by stripping the fixed suffix only, so stat names
    that themselves contain underscores (e.g. 'long_balls') stay intact.
    """
    if df is None or not hasattr(df, 'to_dict') or len(df) == 0:
        return []

    out = []
    for rec in df.to_dict('records'):
        period = rec.get('period')
        mid = rec.get('match_id')
        ing = rec.get('ingested_at')
        stats: dict[str, dict] = {}
        for col, val in rec.items():
            if col in _MS_SKIP:
                continue
            suffix = next((s for s in _MS_STAT_SUFFIXES if col.endswith(s)), None)
            if not suffix:
                continue
            stat = col[: -len(suffix)]
            slot = _MS_SUFFIX_SLOT[suffix]
            stats.setdefault(stat, {})[slot] = (None if val is None else str(val))
        for stat, vals in stats.items():
            out.append({
                'combo_id':   combo_id,
                'match_id':   mid,
                'period':     period,
                'stat_group': None,          # wide source carries no group label
                'stat_name':  stat,
                'home_value': vals.get('home_value'),
                'away_value': vals.get('away_value'),
                'home_total': vals.get('home_total'),
                'away_total': vals.get('away_total'),
                'ingested_at': ing,
            })
    return out


_ODDS_SKIP = {'match_id', 'combo_id', 'ingested_at'}


def _melt_sofa_odds_long(df, combo_id):
    """
    Reshape wide Sofascore odds (one column per selection) into long rows:
    one row per non-null selection. Lossless and robust — the full selection
    label is kept verbatim (oddsp_odds remains the decomposed primary source).
    """
    if df is None or not hasattr(df, 'to_dict') or len(df) == 0:
        return []

    out = []
    for rec in df.to_dict('records'):
        mid = rec.get('match_id')
        ing = rec.get('ingested_at')
        for col, val in rec.items():
            if col in _ODDS_SKIP or val is None:
                continue
            out.append({
                'combo_id':    combo_id,
                'match_id':    mid,
                'selection':   col,
                'odds_value':  str(val),
                'ingested_at': ing,
            })
    return out


# read_json_data.py emits the provider-native primary key as `id`, which would
# collide with the BIGSERIAL surrogate `id`. Remap it to the natural key column.
_SOFA_ID_REMAP = {
    'sofa_matches':  'match_id',
    'sofa_seasons':  'season_id',
    'sofa_referees': 'referee_id',
    'sofa_managers': 'manager_id',
    'sofa_shots':    'shot_id',
}


# ══════════════════════════════════════════════════════════════════════════════
# PROVIDER LOADERS
# ══════════════════════════════════════════════════════════════════════════════

def _load_whoscored(cur, ws_data, ws_id, combo_id, schema):
    """Run all WhoScored parsers and push to ws_* tables."""
    from include.helpers.whoscored.parse_lineups import parse_lineups
    from include.helpers.whoscored.parse_passing import parse_passing
    from include.helpers.whoscored.parse_passing_advanced import parse_passing_advanced
    from include.helpers.whoscored.parse_defending import parse_defending
    from include.helpers.whoscored.parse_goalkeeping import parse_goalkeeping
    from include.helpers.whoscored.parse_discipline import parse_discipline
    from include.helpers.whoscored.parse_possession import parse_possession
    from include.helpers.whoscored.parse_advanced_possession import parse_possession_advanced

    total = 0

    # ── Match metadata ────────────────────────────────────────────────
    home = ws_data['home']
    away = ws_data['away']
    meta_row = {
        'whoscored_match_id': ws_id,
        'combo_id':           combo_id,
        'home_team':          home['name'],
        'home_team_id':       home['teamId'],
        'away_team':          away['name'],
        'away_team_id':       away['teamId'],
        'score':              ws_data.get('score'),
        'ht_score':           ws_data.get('htScore'),
        'venue':              ws_data.get('venueName'),
        'attendance':         ws_data.get('attendance'),
        'referee':            (ws_data.get('referee') or {}).get('name'),
        'start_time':         ws_data.get('startTime'),
    }
    _push_table(cur, 'ws_match_meta', [meta_row], ['combo_id'], schema, combo_id)
    total += 1

    # ── Lineups + formations ──────────────────────────────────────────
    lineup_result = parse_lineups(ws_data, ws_id, combo_id)

    rows = [remap_row(r, combo_id) for r in lineup_result.get('player', [])]
    total += _push_table(cur, 'ws_lineups', rows, ['combo_id', 'player_id'], schema, combo_id)

    rows = [remap_row(r, combo_id) for r in lineup_result.get('formations', [])]
    total += _push_table(cur, 'ws_formations', rows,
                         ['combo_id', 'team_id', 'formation_index'], schema, combo_id)

    # ── Team + player stats from parsers ──────────────────────────────
    stat_parsers = [
        (parse_passing,              'ws_team_passing',     'ws_player_passing',     'team', 'player'),
        (parse_passing_advanced,     'ws_team_passing_adv', 'ws_player_passing_adv', 'team', 'player'),
        (parse_defending,            'ws_team_defending',   'ws_player_defending',   'team', 'player'),
        (parse_discipline,           'ws_team_discipline',  'ws_player_discipline',  'team', 'player'),
        (parse_possession,           'ws_team_possession',  'ws_player_possession',  'team', 'player'),
        (parse_possession_advanced,  'ws_team_poss_adv',    None,                    'team', None),
        (parse_goalkeeping,          None,                  'ws_player_goalkeep',    None,   'keepers'),
    ]

    for parser_fn, team_table, player_table, team_key, player_key in stat_parsers:
        try:
            result = parser_fn(ws_data, ws_id)

            if team_table and team_key and result.get(team_key):
                rows = [remap_row(r, combo_id) for r in result[team_key]]
                # Strip player fields from team rows
                for r in rows:
                    r.pop('player_id', None)
                    r.pop('player_name', None)
                total += _push_table(cur, team_table, rows,
                                     ['combo_id', 'team_id'], schema, combo_id)

            if player_table and player_key and result.get(player_key):
                rows = [remap_row(r, combo_id) for r in result[player_key]]
                # Strip non-derivable columns from goalkeeping
                if player_table == 'ws_player_goalkeep':
                    for r in rows:
                        r.pop('xgot', None)
                        r.pop('psxg_minus_ga', None)
                total += _push_table(cur, player_table, rows,
                                     ['combo_id', 'player_id'], schema, combo_id)

            # Special: sequences
            if result.get('sequences'):
                rows = [remap_row(r, combo_id) for r in result['sequences']]
                total += _push_table(cur, 'ws_sequences', rows, [], schema,
                                     combo_id, delete_first=True)

            # Special: pass_map, pass_network, avg_positions
            if result.get('pass_map'):
                rows = [{**remap_row(r, combo_id), 'whoscored_match_id': ws_id}
                        for r in result['pass_map']]
                total += _push_table(cur, 'ws_pass_map', rows, [], schema,
                                     combo_id, delete_first=True)

            if result.get('pass_network'):
                rows = [{**remap_row(r, combo_id), 'whoscored_match_id': ws_id}
                        for r in result['pass_network']]
                total += _push_table(cur, 'ws_pass_network', rows,
                                     ['combo_id', 'passer_id', 'receiver_id'], schema, combo_id)

            if result.get('player_positions'):
                rows = [{**remap_row(r, combo_id), 'whoscored_match_id': ws_id}
                        for r in result['player_positions']]
                total += _push_table(cur, 'ws_avg_positions', rows,
                                     ['combo_id', 'player_id'], schema, combo_id)

        except Exception as e:
            log.error(f'  WS parser for {team_table or player_table} failed: {e}')

    log.info(f'  WhoScored: {total} rows loaded')
    return total


def _load_fotmob(cur, fm_data, fm_id, combo_id, schema):
    """Run Fotmob parser and push to fot_* tables."""
    from include.helpers.fotmob.parse_fotmob import parse_fotmob

    result = parse_fotmob(fm_data, fm_id, combo_id)
    total = 0

    # ── Fold coaches into the match row (fot_coaches table removed) ────
    # parse_fotmob still returns a 'coaches' list (home + away); collapse it
    # into ht_/at_ columns on the single match row instead of its own table.
    coaches = result.get('coaches') or []
    match_rows = result.get('match') or []
    if match_rows:
        m = match_rows[0]
        for c in coaches:
            if c.get('is_home_team'):
                m['ht_coach_id'] = c.get('coach_id')
                m['ht_coach_name'] = c.get('coach_name')
            else:
                m['at_coach_id'] = c.get('coach_id')
                m['at_coach_name'] = c.get('coach_name')

    # ── Rename the reserved word `group` → `stat_group` on team stats ──
    for r in (result.get('team_stats') or []):
        if 'group' in r:
            r['stat_group'] = r.pop('group')

    # Tables with their conflict keys (fot_coaches intentionally absent)
    table_config = {
        'match':        ('fot_match',        ['combo_id']),
        'lineups':      ('fot_lineups',      ['combo_id', 'player_id']),
        'player_stats': ('fot_player_stats', ['combo_id', 'player_id']),
        'team_stats':   ('fot_team_stats',   []),  # delete + insert
        'shots':        ('fot_shots',        []),  # delete + insert
        'spatial':      ('fot_spatial',      ['combo_id']),
    }

    for result_key, (table, conflict) in table_config.items():
        rows = result.get(result_key, [])
        if not rows:
            continue

        delete_first = (conflict == [])
        total += _push_table(cur, table, rows, conflict, schema,
                             combo_id, delete_first=delete_first)

    log.info(f'  Fotmob: {total} rows loaded')
    return total


def _load_sofascore(cur, sf_dir, combo_id, schema):
    """Run Sofascore parsers (read_json_data.py) and push to sofa_* tables."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

    try:
        from include.helpers.sofascore import read_json_data as rd_json
    except ImportError:
        log.error('Cannot import read_json_data')
        return 0

    def find_sofa(pattern):
        for f in os.listdir(sf_dir):
            if pattern in f and f.endswith('.json'):
                return os.path.join(sf_dir, f)
        return None

    # Find all Sofascore files
    lineups_path = find_sofa('lineups')
    main_path = find_sofa('main_event_data')
    shotmap_path = find_sofa('shotmap')
    stats_path = find_sofa('statistics')
    managers_path = find_sofa('managers')
    odds_path = find_sofa('odds_all')
    best_players_path = find_sofa('best_players_summary')
    avg_pos_path = find_sofa('average_positions')
    comments_path = find_sofa('comments')
    graph_path = find_sofa('graph')
    heatmap_home_path = find_sofa('heatmap_home_team')
    heatmap_away_path = find_sofa('heatmap_away_team')

    if not lineups_path or not main_path:
        log.warning('  Sofascore: missing required files, skipping')
        return 0

    total = 0

    # Parse all sofascore data — each returns a DataFrame
    # Convert to list of dicts for upsert
    try:
        player_data = rd_json.players(lineups_path)
        tournament_data = rd_json.tournament_and_season(main_path)
        teams_and_ref = rd_json.teams_and_referee(main_path)
        mgr = rd_json.managers(managers_path)
        match_det = rd_json.match_details(
            main_path, best_players_path,
            mgr['home_manager_id'], mgr['away_manager_id'],
            tournament_data['tournament_id'], tournament_data['season_id'],
            combo_id,
            player_data['home_formation'], player_data['away_formation'],
        )
        match_id = match_det['match_id']
        home_id = teams_and_ref['home_team_id']
        away_id = teams_and_ref['away_team_id']
    except Exception as e:
        log.error(f'  Sofascore core parsing failed: {e}')
        return 0

    # Map of parser result → (table_name, conflict_keys)
    # Each value is a DataFrame — convert to records for upsert
    df_tables = {
        'sofa_tournaments':    (tournament_data['tournament'],    ['tournament_id']),
        'sofa_seasons':        (tournament_data['season'],        ['season_id']),
        'sofa_teams':          (teams_and_ref['teams'],           ['team_id']),
        'sofa_players':        (player_data['players'],           ['player_id']),
        'sofa_referees':       (teams_and_ref['referee'],         ['referee_id']),
        'sofa_managers':       (mgr['managers'],                  ['manager_id']),
        'sofa_matches':        (match_det['match'],               ['combo_id']),
    }

    # Optional tables
    try:
        df_tables['sofa_lineups'] = (
            rd_json.lineups_table(lineups_path, match_id, combo_id, home_id, away_id),
            []
        )
    except Exception:
        pass

    try:
        df_tables['sofa_missing_players'] = (
            rd_json.missing_players(lineups_path, match_id, combo_id, home_id, away_id),
            []
        )
    except Exception:
        pass

    try:
        df_tables['sofa_player_stats'] = (
            rd_json.player_stats(lineups_path, match_id, combo_id, home_id, away_id),
            []
        )
    except Exception:
        pass

    if shotmap_path:
        try:
            df_tables['sofa_shots'] = (
                rd_json.shots_table(shotmap_path, match_id, combo_id),
                []
            )
        except Exception:
            pass

    if stats_path:
        try:
            df_tables['sofa_match_stats'] = (
                _melt_match_stats_long(
                    rd_json.match_stats(stats_path, match_id, combo_id),
                    combo_id,
                ),
                []
            )
        except Exception:
            pass

    if odds_path:
        try:
            df_tables['sofa_odds'] = (
                _melt_sofa_odds_long(
                    rd_json.odds_table(odds_path, combo_id, match_id),
                    combo_id,
                ),
                []
            )
        except Exception:
            pass

    if all([avg_pos_path, comments_path, graph_path, heatmap_home_path, heatmap_away_path]):
        try:
            df_tables['sofa_spatial'] = (
                rd_json.misc_json_data(
                    avg_pos_path, comments_path, graph_path,
                    heatmap_home_path, heatmap_away_path,
                    match_id, combo_id, full_heatmaps={'None': None},
                ),
                []  # delete+insert, no upsert conflict
            )
        except Exception:
            pass

    # ── Coverage flags on sofa_matches (ported from test_parsers Issue 4) ──
    # Sofascore's match JSON doesn't reliably carry hasShotmap/hasXg across
    # seasons, so derive them from what actually parsed: has_shotmap is true
    # when the shotmap produced rows; has_xg when those shots carry a
    # populated xg/expected column.
    if 'sofa_matches' in df_tables:
        match_df = df_tables['sofa_matches'][0]
        shots_df = df_tables.get('sofa_shots', (None,))[0]
        has_shotmap = shots_df is not None and len(shots_df) > 0
        has_xg = False
        if has_shotmap and hasattr(shots_df, 'columns'):
            xg_cols = [c for c in shots_df.columns
                       if 'xg' in c.lower() or 'expected' in c.lower()]
            if xg_cols:
                has_xg = bool(shots_df[xg_cols[0]].notna().any())
        if hasattr(match_df, 'loc'):
            match_df['has_shotmap'] = has_shotmap
            match_df['has_xg'] = has_xg

    # Push all DataFrames to Postgres
    for table_name, (df, conflict_cols) in df_tables.items():
        try:
            if df is None or (hasattr(df, '__len__') and len(df) == 0):
                continue

            # Convert DataFrame to list of dicts
            if hasattr(df, 'to_dict'):
                rows = df.to_dict('records')
            elif isinstance(df, list):
                rows = df
            else:
                continue

            # Remap provider-native `id` → natural key to avoid colliding
            # with the BIGSERIAL surrogate primary key.
            natural = _SOFA_ID_REMAP.get(table_name)
            if natural:
                for r in rows:
                    if 'id' in r:
                        r[natural] = r.pop('id')

            # Tables with a conflict key upsert; keyless event/per-match tables
            # are delete-by-combo then re-insert for idempotency.
            delete_first = (conflict_cols == [])
            total += _push_table(cur, table_name, rows, conflict_cols, schema,
                                 combo_id, delete_first=delete_first)
        except Exception as e:
            log.error(f'  Sofascore {table_name} push failed: {e}')

    log.info(f'  Sofascore: {total} rows loaded')
    return total


def _load_oddspedia(cur, odds_dir, combo_id, schema):
    """Run Oddspedia parser and push to oddsp_odds."""
    from include.helpers.oddspedia.parse_oddspedia import parse_oddspedia

    result = parse_oddspedia(odds_dir, combo_id)
    rows = result.get('odds', [])

    if not rows:
        return 0

    # Bookmaker detail intentionally dropped — we don't track per-book odds.
    for r in rows:
        r.pop('bookmaker_name', None)
        r.pop('bookmaker_slug', None)

    total = _push_table(cur, 'oddsp_odds', rows, [], schema,
                        combo_id, delete_first=True)

    log.info(f'  Oddspedia: {total} rows loaded')
    return total


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def load_match(match_descriptor: dict, conn, schema: str = 'raw'):
    """
    Load a single match from all available providers into the RAW schema.

    Uses PostgreSQL SAVEPOINTs to isolate each provider. If Fotmob fails,
    WhoScored data is preserved and Sofascore/Oddspedia can still load.

    Raises if ANY provider fails so the Airflow task is marked as failed.

    Args:
        match_descriptor: Dict from discovery with file paths and flags.
        conn:   psycopg2 connection (caller manages commit/rollback).
        schema: Target Postgres schema (default: 'raw').
    """
    cur = conn.cursor()
    combo_id = match_descriptor['combo_id']
    total = 0
    failures = []

    def _run_provider(name, fn, *args):
        """Run a provider loader inside a SAVEPOINT."""
        nonlocal total
        cur.execute(f'SAVEPOINT before_{name}')
        try:
            count = fn(cur, *args)
            cur.execute(f'RELEASE SAVEPOINT before_{name}')
            total += count
        except Exception as e:
            cur.execute(f'ROLLBACK TO SAVEPOINT before_{name}')
            log.error(f'[{combo_id}] {name} load failed: {e}')
            failures.append(name)

    # ── WhoScored (required) ─────────────────────────────────────────
    ws_path = match_descriptor.get('ws_path')
    ws_data = _load_json(ws_path)
    if not ws_data:
        raise ValueError(f'[{combo_id}] No WhoScored data — cannot proceed')

    ws_id = _extract_id_from_filename(ws_path, r'whoscored_(\d+)\.json')
    _run_provider('whoscored', _load_whoscored, ws_data, ws_id, combo_id, schema)

    # ── Fotmob (optional) ─────────────────────────────────────────────
    fm_path = match_descriptor.get('fm_path')
    fm_data = _load_json(fm_path)
    if fm_data:
        fm_id = _extract_id_from_filename(fm_path, r'fotmob_(\d+)\.json')
        _run_provider('fotmob', _load_fotmob, fm_data, fm_id, combo_id, schema)

    # ── Sofascore (optional) ──────────────────────────────────────────
    sf_dir = match_descriptor.get('sf_dir')
    if sf_dir and os.path.isdir(sf_dir):
        _run_provider('sofascore', _load_sofascore, sf_dir, combo_id, schema)

    # ── Oddspedia (optional) ──────────────────────────────────────────
    odds_dir = match_descriptor.get('odds_dir')
    if odds_dir and os.path.isdir(odds_dir):
        _run_provider('oddspedia', _load_oddspedia, odds_dir, combo_id, schema)

    cur.close()
    log.info(f'[{combo_id}] Total: {total} rows loaded across all providers')

    if failures:
        raise RuntimeError(
            f'[{combo_id}] {len(failures)} provider(s) failed: {", ".join(failures)}'
        )