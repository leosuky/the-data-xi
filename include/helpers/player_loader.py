"""
include/helpers/player_loader.py
===============================
Player-grain loader — the sibling of loader.py, which is match-grain.

Where load_match() keys everything on combo_id, this keys on fotmob_player_id:
one Fotmob player page in, one meta row + N position rows + N career rows out.

Idempotent: meta upserts on fotmob_player_id; positions and career are
delete-by-player then insert, so a player who changed club or picked up a new
position gets a clean rewrite rather than stale rows accumulating.
"""

import os
import re
import json
import logging

from include.helpers.common.db import upsert, get_table_columns

log = logging.getLogger(__name__)

PLAYER_FILE_RE = re.compile(r'^fotmob_(\d+)\.json$')


# ── discovery ────────────────────────────────────────────────────────────────
def discover_player_files(players_dir: str) -> list[dict]:
    """
    Scan a directory of Fotmob player pages (fotmob_{id}.json).

    Returns descriptors for Airflow dynamic mapping, sorted by id. Files that
    aren't valid JSON are skipped with a warning rather than failing the scan.
    """
    if not os.path.isdir(players_dir):
        log.error(f'Players directory not found: {players_dir}')
        return []

    found = []
    for fname in sorted(os.listdir(players_dir)):
        m = PLAYER_FILE_RE.match(fname)
        if not m:
            continue
        found.append({
            'fotmob_player_id': int(m.group(1)),
            'path': os.path.join(players_dir, fname),
        })

    log.info(f'{len(found)} player files found in {players_dir}')
    return found


# ── push helpers ─────────────────────────────────────────────────────────────
def _filter_cols(cur, table, rows, schema):
    """Drop keys the table doesn't have, so a payload change can't break the load."""
    if not rows:
        return []
    valid = {c.lower() for c in get_table_columns(cur, table, schema)}
    return [{k: v for k, v in r.items() if k.lower() in valid} for r in rows]


def _push(cur, table, rows, conflict_cols, schema, player_id, delete_first=False):
    """Push rows inside a SAVEPOINT so one table failing doesn't kill the player."""
    rows = _filter_cols(cur, table, rows, schema)
    if not rows:
        return 0

    cur.execute(f'SAVEPOINT before_{table}')
    try:
        if delete_first:
            cur.execute(
                f'DELETE FROM {schema}.{table} WHERE fotmob_player_id = %s',
                (player_id,))
        for row in rows:
            upsert(cur, table, row, conflict_cols, schema)
        cur.execute(f'RELEASE SAVEPOINT before_{table}')
        return len(rows)
    except Exception as e:
        cur.execute(f'ROLLBACK TO SAVEPOINT before_{table}')
        log.error(f'  {table} push failed for player {player_id}: {e}')
        raise


# ── entry point ──────────────────────────────────────────────────────────────
def load_player(descriptor: dict, conn, schema: str = 'raw'):
    """
    Parse and load one Fotmob player page.

    Coaches load fine: they get a meta row with is_coach=true and simply no
    position rows, so the caller needs no special-casing.

    Args:
        descriptor: {'fotmob_player_id': int, 'path': str} from discovery.
        conn:       psycopg2 connection (caller manages commit/rollback).
        schema:     Target Postgres schema (default: 'raw').
    """
    from include.helpers.fotmob.parse_fotmob_player import parse_fotmob_player

    path = descriptor['path']
    pid = descriptor['fotmob_player_id']

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    result = parse_fotmob_player(data, pid)
    cur = conn.cursor()
    total = 0

    total += _push(cur, 'fot_player_meta', result['player'],
                   ['fotmob_player_id'], schema, pid)

    # delete-then-insert: a player's position mix and club history change
    total += _push(cur, 'fot_player_positions', result['positions'],
                   ['fotmob_player_id', 'position_key'], schema, pid,
                   delete_first=True)
    total += _push(cur, 'fot_player_career', result['career'],
                   ['fotmob_player_id', 'team_id', 'start_date'], schema, pid,
                   delete_first=True)

    row = result['player'][0]
    kind = 'coach' if row.get('is_coach') else f"{len(result['positions'])} positions"
    log.info(f"[{pid}] {row.get('player_name')}: {total} rows ({kind})")

    cur.close()
    return total
