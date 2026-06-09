"""
parse_fotmob.py
---------------
Extracts match data from Fotmob matchDetails JSON.

Handles data availability differences across seasons:
    - Shotmap (xG/xGOT): ABSENT for 2018/19, present from 2019/20+
    - Momentum:          ABSENT pre-2022/23, present from 2022/23+
    - Lineup age/country: ABSENT in older seasons
    - All other sections: Available across all seasons

Produces:
    'match'        — match metadata (1 row)
    'lineups'      — one row per player (starters + subs + unavailable)
    'coaches'      — one row per coach (2 rows)
    'player_stats' — one row per player with flattened stat groups
    'team_stats'   — one row per stat per period (home/away values side by side)
    'shots'        — one row per shot with xG/xGOT (empty for old seasons)
    'spatial'      — momentum data as JSONB blob (empty pre-2022/23)
"""

import re


# ── Fotmob position ID mappings ──────────────────────────────────────────────
# Definitive mapping from Fotmob's own database via playerData API
# (positionDescription.positions). Extracted by fetching all player profiles
# from fotmob_4535604.json and fotmob_2846928.json.
#
# Fotmob's labels are generic (CB, CM, AM, ST) without L/R/C distinction.
# We add lateral specificity using the lineup's horizontalLayout.y coordinate
# at parse time — e.g. CM at y=0.79 becomes RCM, CM at y=0.21 becomes LCM.

FOTMOB_POSITION_MAP = {
     11: 'GK',   # Keeper
     32: 'RB',   # Right Back
     33: 'CB',   # Center Back
     34: 'CB',   # Center Back
     35: 'CB',   # Center Back
     36: 'CB',   # Center Back
     37: 'CB',   # Center Back
     38: 'LB',   # Left Back
     51: 'RWB',  # Right Wing-Back
     55: 'CB',   # Center Back
     62: 'RWB',  # Right Wing-Back
     63: 'DM',   # Defensive Midfielder
     64: 'DM',   # Defensive Midfielder
     65: 'DM',   # Defensive Midfielder
     66: 'DM',   # Defensive Midfielder
     68: 'LWB',  # Left Wing-Back
     71: 'RM',   # Right Midfielder
     72: 'RM',   # Right Midfielder
     73: 'CM',   # Central Midfielder
     74: 'CM',   # Central Midfielder
     75: 'CM',   # Central Midfielder
     77: 'CM',   # Central Midfielder
     78: 'LM',   # Left Midfielder
     79: 'LM',   # Left Midfielder
     82: 'RW',   # Right Winger
     83: 'RW',   # Right Winger
     84: 'AM',   # Attacking Midfielder
     85: 'AM',   # Attacking Midfielder
     86: 'AM',   # Attacking Midfielder
     87: 'LW',   # Left Winger
     88: 'LW',   # Left Winger
     95: 'AM',   # Attacking Midfielder
    103: 'RW',   # Right Winger
    104: 'ST',   # Striker
    105: 'ST',   # Striker
    106: 'ST',   # Striker
    107: 'LW',   # Left Winger
    115: 'ST',   # Striker
}

# Positions where y-coordinate can add L/R/C specificity
_LATERALIZABLE = {'CB', 'DM', 'CM', 'AM', 'ST'}

USUAL_POSITION_MAP = {
    0: 'GK',
    1: 'DEF',
    2: 'MID',
    3: 'FWD',
}


def _get_position_label(position_id: int | None, y_coord: float | None = None) -> str | None:
    """
    Map a Fotmob positionId to a position label with lateral specificity.

    Uses Fotmob's definitive base label, then refines with the lineup's
    y-coordinate (from horizontalLayout) for positions that could be
    left/center/right (CB, DM, CM, AM, ST).

    Args:
        position_id:  Fotmob positionId (e.g. 83)
        y_coord:      Lateral coordinate from horizontalLayout.y
                      (0.0=left touchline, 1.0=right touchline)

    Returns:
        Position label (e.g. 'RCM', 'LCB', 'CAM', 'RW') or None if unknown.

    Examples:
        _get_position_label(73, y_coord=0.79)  → 'RCM'
        _get_position_label(75, y_coord=0.50)  → 'CM'
        _get_position_label(77, y_coord=0.21)  → 'LCM'
        _get_position_label(83, y_coord=0.83)  → 'RW'  (already lateral, no change)
        _get_position_label(11)                → 'GK'
    """
    if position_id is None:
        return None

    base = FOTMOB_POSITION_MAP.get(position_id)
    if not base:
        return None

    # Add L/R/C prefix for generic positions using y-coordinate
    if base in _LATERALIZABLE and y_coord is not None:
        if y_coord > 0.65:
            return f'R{base}'   # Right side
        elif y_coord < 0.35:
            return f'L{base}'   # Left side
        else:
            return f'C{base}' if base in ('DM', 'AM') else base  # Center

    return base


def _safe_get(d: dict, *keys, default=None):
    """Safely traverse nested dicts."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k, default)
    return d


# ── Match metadata ────────────────────────────────────────────────────────────

def _parse_match(data: dict, fotmob_id: int, combo_id: str) -> list[dict]:
    """Extract match metadata from general + matchFacts sections."""
    general = data.get('general', {})
    header = data.get('header', {})
    teams = header.get('teams', [])
    info_box = _safe_get(data, 'content', 'matchFacts', 'infoBox', default={})

    home_team = teams[0] if len(teams) > 0 else {}
    away_team = teams[1] if len(teams) > 1 else {}

    # MOTM
    motm = _safe_get(data, 'content', 'matchFacts', 'playerOfTheMatch', default={})

    # Coverage flags — signals what data is available for this match
    has_shot_data = bool(_safe_get(data, 'content', 'shotmap', 'shots'))
    has_momentum  = bool(_safe_get(data, 'content', 'momentum', 'main', 'data'))

    # Team colors
    colors = general.get('teamColors', {})

    return [{
        'fotmob_id':         fotmob_id,
        'combo_id':          combo_id,
        'match_name':        general.get('matchName'),
        'match_round':       general.get('matchRound'),
        'league_id':         general.get('leagueId'),
        'league_name':       general.get('leagueName'),
        'league_round_name': general.get('leagueRoundName'),
        'country_code':      _safe_get(data, 'content', 'matchFacts', 'countryCode'),
        'home_team_id':      home_team.get('id'),
        'home_team_name':    home_team.get('name'),
        'home_score':        home_team.get('score'),
        'away_team_id':      away_team.get('id'),
        'away_team_name':    away_team.get('name'),
        'away_score':        away_team.get('score'),
        'match_date':        _safe_get(info_box, 'Match Date', 'utcTime'),
        'stadium_name':      _safe_get(info_box, 'Stadium', 'name'),
        'stadium_city':      _safe_get(info_box, 'Stadium', 'city'),
        'stadium_country':   _safe_get(info_box, 'Stadium', 'country'),
        'stadium_capacity':  _safe_get(info_box, 'Stadium', 'capacity'),
        'attendance':        info_box.get('Attendance'),
        'referee_name':      _safe_get(info_box, 'Referee', 'text'),
        'referee_country':   _safe_get(info_box, 'Referee', 'country'),
        'motm_id':           motm.get('id'),
        'motm_name':         _safe_get(motm, 'name', 'fullName'),
        'motm_rating':       _safe_get(motm, 'rating', 'num'),
        'home_color':        _safe_get(colors, 'lightMode', 'home'),
        'away_color':        _safe_get(colors, 'lightMode', 'away'),
        'home_color_dark':   _safe_get(colors, 'darkMode', 'home'),
        'away_color_dark':   _safe_get(colors, 'darkMode', 'away'),
        'home_font_color':   _safe_get(colors, 'fontLightMode', 'home'),
        'away_font_color':   _safe_get(colors, 'fontLightMode', 'away'),
        'home_font_dark':    _safe_get(colors, 'fontDarkMode', 'home'),
        'away_font_dark':    _safe_get(colors, 'fontDarkMode', 'away'),
        'has_shot_data':     has_shot_data,
        'has_momentum_data': has_momentum,
    }]


# ── Lineups ───────────────────────────────────────────────────────────────────

def _parse_lineups(data: dict, fotmob_id: int, combo_id: str) -> list[dict]:
    """Extract lineup data — starters, subs, and unavailable players."""
    lineup = _safe_get(data, 'content', 'lineup', default={})
    rows = []

    for side, team_key in [('home', 'homeTeam'), ('away', 'awayTeam')]:
        team = lineup.get(team_key, {})
        team_id = team.get('id')
        team_name = team.get('name')
        formation = team.get('formation')
        is_home = (side == 'home')

        # Starters
        for p in team.get('starters', []):
            pos_id = p.get('positionId')
            usual_id = p.get('usualPlayingPositionId')
            y_coord = (p.get('horizontalLayout') or {}).get('y')
            rows.append({
                'fotmob_id':         fotmob_id,
                'combo_id':          combo_id,
                'player_id':         p.get('id'),
                'player_name':       p.get('name'),
                'team_id':           team_id,
                'team_name':         team_name,
                'is_home_team':      is_home,
                'role':              'starter',
                'shirt_number':      p.get('shirtNumber'),
                'position_id':       pos_id,
                'position':          _get_position_label(pos_id, y_coord),
                'usual_position_id': usual_id,
                'usual_position':    USUAL_POSITION_MAP.get(usual_id),
                'is_captain':        p.get('isCaptain', False),
                'age':               p.get('age'),
                'country':           p.get('countryName'),
                'country_code':      p.get('countryCode'),
                'rating':            _safe_get(p, 'performance', 'rating'),
                'formation':         formation,
            })

        # Subs (no y-coordinate — not positioned in formation)
        for p in team.get('subs', []):
            pos_id = p.get('positionId')
            usual_id = p.get('usualPlayingPositionId')
            rows.append({
                'fotmob_id':         fotmob_id,
                'combo_id':          combo_id,
                'player_id':         p.get('id'),
                'player_name':       p.get('name'),
                'team_id':           team_id,
                'team_name':         team_name,
                'is_home_team':      is_home,
                'role':              'sub',
                'shirt_number':      p.get('shirtNumber'),
                'position_id':       pos_id,
                'position':          _get_position_label(pos_id),
                'usual_position_id': usual_id,
                'usual_position':    USUAL_POSITION_MAP.get(usual_id),
                'is_captain':        p.get('isCaptain', False),
                'age':               p.get('age'),
                'country':           p.get('countryName'),
                'country_code':      p.get('countryCode'),
                'rating':            _safe_get(p, 'performance', 'rating'),
                'formation':         formation,
            })

        # Unavailable
        for p in team.get('unavailable', []):
            unavail = p.get('unavailability', {})
            pos_id = p.get('positionId')
            usual_id = p.get('usualPlayingPositionId')
            rows.append({
                'fotmob_id':          fotmob_id,
                'combo_id':           combo_id,
                'player_id':          p.get('id'),
                'player_name':        p.get('name'),
                'team_id':            team_id,
                'team_name':          team_name,
                'is_home_team':       is_home,
                'role':               'unavailable',
                'shirt_number':       None,
                'position_id':        pos_id,
                'position':           _get_position_label(pos_id),
                'usual_position_id':  usual_id,
                'usual_position':     USUAL_POSITION_MAP.get(usual_id),
                'is_captain':         False,
                'age':                p.get('age'),
                'country':            p.get('countryName'),
                'country_code':       p.get('countryCode'),
                'rating':             None,
                'formation':          formation,
                'unavailable_type':   unavail.get('type'),
                'unavailable_reason': unavail.get('reason'),
            })

    return rows


# ── Coaches ───────────────────────────────────────────────────────────────────

def _parse_coaches(data: dict, fotmob_id: int, combo_id: str) -> list[dict]:
    """Extract coach/manager info from lineup data."""
    lineup = _safe_get(data, 'content', 'lineup', default={})
    rows = []

    for side, team_key in [('home', 'homeTeam'), ('away', 'awayTeam')]:
        team = lineup.get(team_key, {})
        coach = team.get('coach', {})
        if coach:
            rows.append({
                'fotmob_id':     fotmob_id,
                'combo_id':      combo_id,
                'coach_id':      coach.get('id'),
                'coach_name':    coach.get('name'),
                'team_id':       team.get('id'),
                'team_name':     team.get('name'),
                'is_home_team':  (side == 'home'),
                'country':       coach.get('countryName'),
                'country_code':  coach.get('countryCode'),
                'age':           coach.get('age'),
            })

    return rows


# ── Player stats ──────────────────────────────────────────────────────────────

def _parse_player_stats(data: dict, fotmob_id: int, combo_id: str) -> list[dict]:
    """
    Flatten per-player stats from the playerStats section.

    Stats are grouped by category (Top stats, Attack, Defense, Duels).
    Each stat has a key and a value (integer, double, or fraction).
    We flatten everything into one row per player.
    """
    ps = _safe_get(data, 'content', 'playerStats', default={})
    rows = []

    for player_id_str, pdata in ps.items():
        row = {
            'fotmob_id':     fotmob_id,
            'combo_id':      combo_id,
            'player_id':     pdata.get('id'),
            'player_name':   pdata.get('name'),
            'team_id':       pdata.get('teamId'),
            'team_name':     pdata.get('teamName'),
            'is_goalkeeper':  pdata.get('isGoalkeeper', False),
            'shirt_number':  pdata.get('shirtNumber'),
            'position_id':   pdata.get('positionId'),
        }

        # Flatten stat groups
        for group in pdata.get('stats', []):
            if not isinstance(group, dict):
                continue
            stats_obj = group.get('stats', {})
            if isinstance(stats_obj, dict):
                for stat_key, stat_data in stats_obj.items():
                    if not isinstance(stat_data, dict):
                        continue
                    key = stat_data.get('key') or stat_data.get('title') or stat_key
                    if not key:
                        continue
                    stat = stat_data.get('stat', {})
                    if not isinstance(stat, dict):
                        continue

                    # Clean the key name for column use
                    col = re.sub(r'[^a-z0-9_]', '_', key.lower().strip())
                    col = re.sub(r'_+', '_', col).strip('_')

                    val = stat.get('value')
                    total = stat.get('total')

                    row[col] = val
                    if total is not None:
                        row[f'{col}_total'] = total

        rows.append(row)

    return rows


# ── Team stats ────────────────────────────────────────────────────────────────

def _parse_team_stats(data: dict, fotmob_id: int, combo_id: str) -> list[dict]:
    """
    Extract team-level stats by period.

    Structure: Periods → {period} → stats → [groups] → each group has stats → [items]
    Each stat item has a 'stats' array of [home_value, away_value].
    Periods: 'All', '1st', '2nd' (when available).
    """
    periods = _safe_get(data, 'content', 'stats', 'Periods', default={})
    rows = []

    for period_name, period_data in periods.items():
        if not isinstance(period_data, dict):
            continue

        # The 'stats' key contains a list of stat groups
        stat_groups = period_data.get('stats', [])
        if not isinstance(stat_groups, list):
            continue

        for group in stat_groups:
            if not isinstance(group, dict):
                continue

            group_title = group.get('title', '')
            group_key = group.get('key', '')

            # Each group has a 'stats' list of individual stat items
            items = group.get('stats', [])
            if not isinstance(items, list):
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue
                # Skip section title rows (type='title' with None values)
                if item.get('type') == 'title':
                    continue

                values = item.get('stats', [None, None])
                home_val = values[0] if len(values) > 0 else None
                away_val = values[1] if len(values) > 1 else None

                rows.append({
                    'fotmob_id':     fotmob_id,
                    'combo_id':      combo_id,
                    'period':        period_name,
                    'stat_group':    group_title,
                    'stat_key':      item.get('key'),
                    'stat_title':    item.get('title'),
                    'home_value':    str(home_val) if home_val is not None else None,
                    'away_value':    str(away_val) if away_val is not None else None,
                    'stat_format':   item.get('format'),
                    'highlighted':   item.get('highlighted'),
                })

    return rows


# ── Shots ─────────────────────────────────────────────────────────────────────

def _parse_shots(data: dict, fotmob_id: int, combo_id: str) -> list[dict]:
    """
    Extract shotmap data with xG/xGOT.
    Returns empty list for older seasons where shotmap is absent.
    """
    shots = _safe_get(data, 'content', 'shotmap', 'shots', default=[])
    rows = []

    for s in shots:
        rows.append({
            'fotmob_id':          fotmob_id,
            'combo_id':           combo_id,
            'shot_id':            s.get('id'),
            'event_type':         s.get('eventType'),
            'team_id':            s.get('teamId'),
            'player_id':          s.get('playerId'),
            'player_name':        s.get('playerName'),
            'x':                  s.get('x'),
            'y':                  s.get('y'),
            'minute':             s.get('min'),
            'minute_added':       s.get('minAdded'),
            'period':             s.get('period'),
            'is_blocked':         s.get('isBlocked'),
            'is_on_target':       s.get('isOnTarget'),
            'is_own_goal':        s.get('isOwnGoal'),
            'is_from_inside_box': s.get('isFromInsideBox'),
            'is_saved_off_line':  s.get('isSavedOffLine'),
            'blocked_x':          s.get('blockedX'),
            'blocked_y':          s.get('blockedY'),
            'goal_crossed_y':     s.get('goalCrossedY'),
            'goal_crossed_z':     s.get('goalCrossedZ'),
            'xg':                 s.get('expectedGoals'),
            'xgot':               s.get('expectedGoalsOnTarget'),
            'shot_type':          s.get('shotType'),
            'situation':          s.get('situation'),
            'keeper_id':          s.get('keeperId'),
            'on_goal_shot':       str(s.get('onGoalShot')) if s.get('onGoalShot') else None,
        })

    return rows


# ── Momentum ──────────────────────────────────────────────────────────────────

def _parse_spatial(data: dict, fotmob_id: int, combo_id: str) -> list[dict]:
    """
    Extract spatial/momentum data as a single JSONB-ready row.
    Returns empty list for seasons pre-2022/23 where momentum is absent.
    """
    main = _safe_get(data, 'content', 'momentum', 'main', default={})
    data_points = main.get('data', [])

    if not data_points:
        return []

    return [{
        'fotmob_id':      fotmob_id,
        'combo_id':       combo_id,
        'momentum_data':  data_points,
    }]


# ══════════════════════════════════════════════════════════════════════════════
# Main entry point
# ══════════════════════════════════════════════════════════════════════════════

def parse_fotmob(data: dict, fotmob_id: int, combo_id: str = '') -> dict:
    """
    Extract all available data from a Fotmob matchDetails JSON.

    Handles missing sections gracefully — older seasons won't have
    shotmap or momentum data, and those tables will be empty lists.

    Args:
        data:       Raw Fotmob matchDetails JSON dict.
        fotmob_id:  Fotmob match ID (extracted from filename).
        combo_id:   Match combo_id (e.g. '2025-05-23ComInt').

    Returns:
        {
            'match':        [1 row],
            'lineups':      [N rows — starters + subs + unavailable],
            'coaches':      [2 rows],
            'player_stats': [N rows — one per player with flattened stats],
            'team_stats':   [N rows — one per stat per period],
            'shots':        [N rows — empty for 2018/19],
            'spatial':      [1 row with momentum JSONB — empty pre-2022/23],
        }
    """
    return {
        'match':        _parse_match(data, fotmob_id, combo_id),
        'lineups':      _parse_lineups(data, fotmob_id, combo_id),
        'coaches':      _parse_coaches(data, fotmob_id, combo_id),
        'player_stats': _parse_player_stats(data, fotmob_id, combo_id),
        'team_stats':   _parse_team_stats(data, fotmob_id, combo_id),
        'shots':        _parse_shots(data, fotmob_id, combo_id),
        'spatial':      _parse_spatial(data, fotmob_id, combo_id),
    }


if __name__ == '__main__':
    import json

    path = './fotmob_4535604.json'
    fm_id = int(path.split('_')[-1].replace('.json', ''))

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    result = parse_fotmob(data, fm_id, combo_id='2025-05-23ComInt')

    for key, rows in result.items():
        print(f'{key}: {len(rows)} rows')