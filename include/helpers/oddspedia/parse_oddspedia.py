"""
parse_oddspedia.py
------------------
Extracts pre-match odds from Oddspedia JSON files (one file per market).

Handles two market types:
    - Fixed outcomes (has_handicap=0): 1X2, BTTS, Double Chance, etc.
      Outcomes are labeled (Home/Draw/Away, Yes/No, etc.)
    - Line/alternative markets (has_handicap=1): Asian Handicap, Total Goals,
      Correct Score, etc. Each line is a separate outcome group.

Only stores the BEST odds per outcome — no per-bookmaker breakdown.

Some files may be empty (market not available for that match):
    {"generated_at": "...", "message": ["#NOWTC3535"]}
These are silently skipped.

Produces:
    'odds' — one row per market × period × outcome (or line)
"""

import json
import os
import re
import logging

log = logging.getLogger(__name__)


def _parse_single_market(data: dict, oddspedia_match_id: str, combo_id: str,
                         market_slug: str) -> list[dict]:
    """
    Parse a single Oddspedia market JSON file into rows.

    Fixed markets produce one row per outcome per period.
    Line markets produce one row per line per period.
    """
    market_data = data.get('data', {})

    # Skip empty/error responses
    if not market_data or 'odds' not in market_data:
        return []

    market_name     = market_data.get('market_name', market_slug)
    market_group_id = market_data.get('market_group_id')
    has_handicap    = market_data.get('has_handicap', 0)
    outcome_names   = market_data.get('outcome_names') or []
    periods_list    = market_data.get('periods', [])

    # Build period ID → name lookup
    period_names = {str(p['id']): p['name'] for p in periods_list}

    rows = []

    for period_id_str, period_odds in market_data.get('odds', {}).items():
        period_name = period_names.get(period_id_str, f'Period {period_id_str}')

        if has_handicap and 'alternative' in period_odds:
            # ── Line / alternative market ─────────────────────────────
            alternatives = period_odds.get('alternative', [])
            for alt in alternatives:
                line_name = alt.get('name') or alt.get('name_en', '')

                for outcome_key, odd_data in alt.get('odds', {}).items():
                    if not isinstance(odd_data, dict):
                        continue

                    # Determine outcome label
                    outcome_idx = int(outcome_key.replace('o', '')) - 1
                    if outcome_names and outcome_idx < len(outcome_names):
                        outcome_label = outcome_names[outcome_idx]
                    else:
                        outcome_label = outcome_key

                    rows.append({
                        'oddspedia_match_id':  oddspedia_match_id,
                        'combo_id':            combo_id,
                        'market_name':         market_name,
                        'market_group_id':     market_group_id,
                        'market_slug':         market_slug,
                        'period_name':         period_name,
                        'line':                line_name,
                        'outcome_name':        outcome_label,
                        'odds_value':          odd_data.get('odds_value'),
                        'bookmaker_name':      odd_data.get('bookie_name'),
                        'bookmaker_slug':      odd_data.get('bookie_slug'),
                        'odds_direction':      odd_data.get('odds_direction'),
                    })

        elif 'odds' in period_odds:
            # ── Fixed outcome market ──────────────────────────────────
            for outcome_key, odd_data in period_odds.get('odds', {}).items():
                if not isinstance(odd_data, dict):
                    continue

                outcome_idx = int(outcome_key.replace('o', '')) - 1
                if outcome_names and outcome_idx < len(outcome_names):
                    outcome_label = outcome_names[outcome_idx]
                else:
                    outcome_label = outcome_key

                rows.append({
                    'oddspedia_match_id':  oddspedia_match_id,
                    'combo_id':            combo_id,
                    'market_name':         market_name,
                    'market_group_id':     market_group_id,
                    'market_slug':         market_slug,
                    'period_name':         period_name,
                    'line':                None,
                    'outcome_name':        outcome_label,
                    'odds_value':          odd_data.get('odds_value'),
                    'bookmaker_name':      odd_data.get('bookie_name'),
                    'bookmaker_slug':      odd_data.get('bookie_slug'),
                    'odds_direction':      odd_data.get('odds_direction'),
                })

    return rows


def parse_oddspedia(odds_dir: str, combo_id: str = '',
                    full_time_only: bool = False) -> dict:
    """
    Parse all Oddspedia odds files in a directory.

    Expects files named: {oddspedia_match_id}_odds_{market_slug}.json

    Args:
        odds_dir:        Path to directory containing odds JSON files.
        combo_id:        Match combo_id (e.g. '2025-05-23ComInt').
        full_time_only:  If True (default), only keeps Full Time period odds.
                         Cuts volume by ~60%.

    Returns:
        {
            'odds': [
                {
                    'oddspedia_match_id', 'combo_id', 'market_name',
                    'market_group_id', 'market_slug', 'period_name',
                    'line', 'outcome_name', 'odds_value',
                    'bookmaker_name', 'bookmaker_slug', 'odds_direction',
                }, ...
            ]
        }
    """
    if not os.path.isdir(odds_dir):
        return {'odds': []}

    all_rows = []
    pattern = re.compile(r'^(\d+)_odds_(.+)\.json$')

    for fname in sorted(os.listdir(odds_dir)):
        m = pattern.match(fname)
        if not m:
            continue

        oddspedia_match_id = m.group(1)
        market_slug = m.group(2)
        fpath = os.path.join(odds_dir, fname)

        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f'Failed to read {fname}: {e}')
            continue

        # Skip empty/error responses
        if 'message' in data and 'data' not in data:
            continue

        rows = _parse_single_market(data, oddspedia_match_id, combo_id, market_slug)
        all_rows.extend(rows)

    # Filter to Full Time only if requested
    if full_time_only:
        all_rows = [r for r in all_rows if r['period_name'] == 'Full Time']

    log.info(f'Oddspedia: {len(all_rows)} odds rows from {odds_dir}')
    return {'odds': all_rows}


if __name__ == '__main__':
    import sys

    odds_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
    result = parse_oddspedia(odds_dir, combo_id='test')

    print(f'Total rows: {len(result["odds"])}')
    markets = set(r['market_name'] for r in result['odds'])
    for market in sorted(markets):
        rows = [r for r in result['odds'] if r['market_name'] == market]
        periods = set(r['period_name'] for r in rows)
        print(f'  {market}: {len(rows)} rows ({", ".join(sorted(periods))})')