"""
include/helpers/common/db.py
============================
Database helper functions for the The Data XI load pipeline.

Provides:
    - upsert / upsert_many : INSERT ... ON CONFLICT DO UPDATE with
      automatic column introspection (unknown columns are auto-added via
      ALTER TABLE ... ADD COLUMN, typed TEXT).
    - remap_row            : Transform parser output for DB insertion
      (rename whoscored_match_id → match_id, inject combo_id).
    - get_table_columns    : Introspect Postgres table columns (cached).

All functions operate on raw psycopg2 connections/cursors for maximum
control and compatibility with Airflow's PostgresHook.
"""

import json
import logging
from typing import Optional

log = logging.getLogger(__name__)

# ── Column introspection cache ────────────────────────────────────────────────

_column_cache: dict[str, set[str]] = {}


def get_table_columns(cur, table: str, schema: str = 'raw') -> set[str]:
    """
    Return the set of column names for a table. Results are cached
    per-session to avoid repeated information_schema queries.

    Args:
        cur:    psycopg2 cursor
        table:  Table name (without schema prefix)
        schema: Postgres schema name

    Returns:
        Set of lowercase column name strings.
    """
    cache_key = f'{schema}.{table}'
    if cache_key not in _column_cache:
        cur.execute(
            'SELECT column_name FROM information_schema.columns '
            'WHERE table_schema = %s AND table_name = %s',
            (schema, table),
        )
        _column_cache[cache_key] = {row[0] for row in cur.fetchall()}
    return _column_cache[cache_key]


def clear_column_cache():
    """Clear the column introspection cache (useful between test runs)."""
    _column_cache.clear()


# ── Row transformation ────────────────────────────────────────────────────────

def remap_row(row: dict, combo_id: Optional[str] = None) -> dict:
    """
    Transform a parser output row for database insertion.

    Operations:
        1. Inject ``combo_id`` if provided and not already present
        2. Convert list/dict values to JSON strings for JSONB columns

    Note: Does NOT rename whoscored_match_id. The RAW schema keeps
    provider-native column names. The old match_id rename was for the
    analytics schema and is no longer used.

    Args:
        row:      Dict from a parser's output (team or player row).
        combo_id: Match combo_id to inject (e.g. '2024-08-17ArsWol').

    Returns:
        New dict (original is not mutated).
    """
    r = dict(row)

    # Inject combo_id
    if combo_id and 'combo_id' not in r:
        r['combo_id'] = combo_id

    # Serialise complex types for JSONB columns
    for k, v in r.items():
        if isinstance(v, (list, dict)):
            r[k] = json.dumps(v)

    return r


# ── Upsert ────────────────────────────────────────────────────────────────────

def upsert(
    cur,
    table: str,
    row: dict,
    conflict_cols: list[str],
    schema: str = 'raw',
    allow_alter: bool = True,
):
    """
    Insert a single row, handling conflicts via ON CONFLICT DO UPDATE.

    Column introspection: any key in ``row`` that is not yet a column in the
    target table is added via ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS``
    (typed TEXT) before insertion. This is the auto-extend behaviour the RAW
    schema comments describe — dynamic provider stats (sofa_player_stats,
    fot_player_stats, season-varying columns) land without a schema migration.
    Set ``allow_alter=False`` to fall back to the old strip-unknown behaviour.

    All identifiers are lowercased (Postgres folds unquoted identifiers) so
    camelCase parser keys like ``totalPass`` map consistently to ``totalpass``
    on both ADD COLUMN and INSERT.

    Args:
        cur:            psycopg2 cursor
        table:          Target table name (without schema prefix)
        row:            Dict of column_name → value
        conflict_cols:  List of columns forming the unique constraint.
                        If empty, performs a plain INSERT (no conflict clause).
        schema:         Postgres schema name.
        allow_alter:    If True, auto-add unknown columns instead of stripping.
    """
    if not row:
        return

    # Normalise keys to lowercase — Postgres folds unquoted identifiers, so
    # this keeps introspection, ALTER and INSERT in agreement.
    row = {str(k).lower(): v for k, v in row.items()}
    conflict_cols = [c.lower() for c in conflict_cols]

    # Normalise values coming from pandas DataFrames (Sofascore path):
    #   - numpy scalars (int64/float64/bool_) → native Python (psycopg2-safe)
    #   - NaN / NaT → NULL
    #   - list/dict → JSON string (for JSONB/TEXT targets)
    for k in list(row.keys()):
        v = row[k]
        if isinstance(v, (list, dict)):
            row[k] = json.dumps(v)
            continue
        if hasattr(v, 'item') and hasattr(v, 'dtype'):  # numpy scalar
            v = v.item()
            row[k] = v
        if hasattr(v, 'isoformat'):                     # datetime / date / Timestamp
            row[k] = v.isoformat()
            continue
        if v is not None and v != v:                    # NaN / NaT
            row[k] = None

    valid_cols = get_table_columns(cur, table, schema)

    # Auto-extend: add any columns the parser produced that don't exist yet.
    if allow_alter:
        missing = [k for k in row if k not in valid_cols]
        if missing:
            cache_key = f'{schema}.{table}'
            for col in missing:
                cur.execute(
                    f'ALTER TABLE {schema}.{table} '
                    f'ADD COLUMN IF NOT EXISTS "{col}" TEXT'
                )
                _column_cache.setdefault(cache_key, set()).add(col)
            valid_cols = _column_cache[cache_key]

    # Filter to columns that exist in the table (after any auto-extend).
    filtered = {k: v for k, v in row.items() if k in valid_cols}
    if not filtered:
        return

    qualified_table = f'{schema}.{table}'
    cols = list(filtered.keys())
    vals = [filtered[c] for c in cols]
    placeholders = ', '.join(['%s'] * len(vals))
    # Quote identifiers so reserved words / odd names are always safe.
    col_list = ', '.join(f'"{c}"' for c in cols)

    if not conflict_cols:
        # Plain INSERT — for tables with BIGSERIAL PKs (shots, sequences, etc.)
        cur.execute(
            f'INSERT INTO {qualified_table} ({col_list}) VALUES ({placeholders})',
            vals,
        )
        return

    conflict = ', '.join(f'"{c}"' for c in conflict_cols)
    updates = ', '.join(
        f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in conflict_cols
    )
    cur.execute(
        f'INSERT INTO {qualified_table} ({col_list}) VALUES ({placeholders}) '
        f'ON CONFLICT ({conflict}) DO '
        + (f'UPDATE SET {updates}' if updates else 'NOTHING'),
        vals,
    )


def upsert_many(
    cur,
    table: str,
    rows: list[dict],
    conflict_cols: list[str],
    schema: str = 'raw',
):
    """
    Upsert multiple rows. Convenience wrapper around upsert().

    Args:
        cur:            psycopg2 cursor
        table:          Target table name (without schema prefix)
        rows:           List of dicts (one per row)
        conflict_cols:  Conflict columns for ON CONFLICT
        schema:         Postgres schema name
    """
    for row in rows:
        upsert(cur, table, row, conflict_cols, schema)


def delete_match_rows(cur, table: str, match_id: int, schema: str = 'raw'):
    """
    Delete all rows for a given match from a table. Used for idempotent
    re-insertion of tables without natural unique keys (shots, sequences,
    pass_map).

    Args:
        cur:       psycopg2 cursor
        table:     Target table name
        match_id:  WhoScored match ID
        schema:    Postgres schema name
    """
    cur.execute(f'DELETE FROM {schema}.{table} WHERE match_id = %s', (match_id,))