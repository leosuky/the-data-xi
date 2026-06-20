"""
parse_discipline.py
-------------------
Parses WhoScored matchCentreData into discipline stats.

EVENT TYPES
-----------
Foul (type 4)
    Both the fouler and the fouled player receive a Foul event, paired via
    OppositeRelatedEvent (QID 233).
    outcome=Successful   → this player WON the foul (was fouled)
    outcome=Unsuccessful → this player COMMITTED the foul
    QID 285=Defensive, QID 286=Offensive (pressing foul in opponent's half)
    QID 264=AerialFoul — foul in an aerial duel
    QID 13=Foul         — physical contact foul (vs technical violation)

Card (type 6)
    QID 31=Yellow, QID 32=Red, QID 33=SecondYellow
    QID 55=RelatedEventId → links to the Foul event that caused the card
    Card x/y = location of the card-giving incident.

OffsideGiven (type 57)
    Player from THIS team was caught offside.
    x/y = position where player was flagged.

OffsidePass (type 55)
    A pass was played to an offside player (attacker's team record).
    QID 7=PlayerCaughtOffside (value = the offside player's ID)
    QID 8=GoalDisallowed — goal was ruled out for offside

OffsideProvoked (type 58)
    This team successfully caught an opponent offside (defensive offside trap).
    Mirrors OffsideGiven on the opponent side.

FOUL ZONES
----------
Own box:         x ≤ 17
Own half:        17 < x ≤ 50
Midfield:        50 < x ≤ 66.6
Danger area:     x > 66.6  (fouls in final third / opponent's box)
"""

from collections import defaultdict


# ── Helpers ───────────────────────────────────────────────────────────────────

def _has_qid(event, qid: int) -> bool:
    return any(q.get('type', {}).get('value') == qid
               for q in event.get('qualifiers', []))

def _get_qid_val(event, qid: int):
    for q in event.get('qualifiers', []):
        if q.get('type', {}).get('value') == qid:
            return q.get('value')
    return None

def _acc(event) -> bool:
    return event.get('outcomeType', {}).get('displayName') == 'Successful'

def _flt(v, default=0.0) -> float:
    try: return float(v)
    except: return default

def _zone(x: float) -> str:
    if x <= 17.0:   return 'own_box'
    if x <= 50.0:   return 'own_half'
    if x <= 66.6:   return 'midfield'
    return 'danger_area'

def _period(e) -> str:
    return e.get('period', {}).get('displayName', '?')

def _is_late(e) -> bool:
    return e.get('expandedMinute', 0) >= 75


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_discipline(data: dict, whoscored_match_id: int) -> dict:
    """
    Parse all discipline stats from WhoScored matchCentreData.

    Returns:
        {
            'team':   [home_row, away_row]
            'player': [...]
            'cards':  [one row per card event — for detailed card analysis]
        }
    """
    events  = data.get('events', [])
    home_id = data['home']['teamId']
    away_id = data['away']['teamId']
    names   = data.get('playerIdNameDictionary', {})

    event_by_id = {e.get('eventId'): e for e in events}

    # ── Card rows (raw, one per card event) ────────────────────────────────────
    card_rows = []
    for c in events:
        if c['type']['displayName'] != 'Card': continue
        pid  = c.get('playerId')
        tid  = c.get('teamId')
        if tid not in (home_id, away_id): continue

        is_yellow  = _has_qid(c, 31)
        is_red     = _has_qid(c, 32)
        is_2nd_y   = _has_qid(c, 33)
        card_type  = ('SecondYellow' if is_2nd_y
                      else 'Red' if is_red
                      else 'Yellow' if is_yellow else 'Unknown')

        # Link back to the foul that caused the card
        rel_val    = _get_qid_val(c, 55)
        foul_event = event_by_id.get(int(float(rel_val))) if rel_val else None
        foul_x     = _flt(foul_event.get('x')) if foul_event else None
        foul_y     = _flt(foul_event.get('y')) if foul_event else None

        card_rows.append({
            'whoscored_match_id': whoscored_match_id,
            'team_id':            tid,
            'player_id':          pid,
            'player_name':        names.get(str(pid)),
            'card_type':          card_type,
            'minute':             c.get('minute'),
            'expanded_minute':    c.get('expandedMinute'),
            'period':             _period(c),
            'x':                  _flt(c.get('x')),
            'y':                  _flt(c.get('y')),
            'foul_x':             foul_x,
            'foul_y':             foul_y,
            'foul_zone':          _zone(foul_x) if foul_x is not None else None,
        })

    # ── Per-team / per-player aggregation ─────────────────────────────────────
    team_stats  = {}
    player_stats = {}

    for tid in (home_id, away_id):
        team_stats[tid] = {
            'whoscored_match_id':       whoscored_match_id,
            'team_id':                  tid,
            # Fouls
            'fouls_committed':          0,
            'fouls_won':                0,
            'fouls_defensive':          0,
            'fouls_offensive':          0,
            'fouls_aerial':             0,
            'fouls_own_box':            0,
            'fouls_own_half':           0,
            'fouls_midfield':           0,
            'fouls_danger_area':        0,
            'fouls_first_half':         0,
            'fouls_second_half':        0,
            'fouls_late_game':          0,
            # Cards
            'yellow_cards':             0,
            'red_cards':                0,
            'second_yellow_cards':      0,
            # Offsides
            'offsides_committed':       0,
            'offsides_caught':          0,
            'goals_disallowed_offside': 0,
        }

    # Foul events
    for e in events:
        if e['type']['displayName'] != 'Foul': continue
        tid = e.get('teamId')
        if tid not in (home_id, away_id): continue
        pid = e.get('playerId')

        x   = _flt(e.get('x', 50))
        zone = _zone(x)
        is_committed = not _acc(e)   # Unsuccessful = committed
        is_won       = _acc(e)       # Successful   = won

        # Ensure player exists in player_stats
        key = (tid, pid)
        if key not in player_stats and pid:
            player_stats[key] = {
                'whoscored_match_id': whoscored_match_id,
                'team_id':   tid,
                'player_id': pid,
                'player_name': names.get(str(pid)),
                'fouls_committed': 0, 'fouls_won': 0,
                'fouls_defensive': 0, 'fouls_offensive': 0, 'fouls_aerial': 0,
                'fouls_own_box': 0, 'fouls_own_half': 0,
                'fouls_midfield': 0, 'fouls_danger_area': 0,
                'fouls_first_half': 0, 'fouls_second_half': 0, 'fouls_late_game': 0,
                'yellow_cards': 0, 'red_cards': 0, 'second_yellow_cards': 0,
                'offsides_committed': 0,
            }

        t = team_stats[tid]
        p = player_stats.get(key)

        if is_committed:
            t['fouls_committed']   += 1
            if p: p['fouls_committed'] += 1
            # Zone (foul committed at this location)
            t[f'fouls_{zone}']     += 1
            if p: p[f'fouls_{zone}'] += 1
            # Timing
            period_name = _period(e)
            if 'FirstHalf' in period_name or period_name == 'FirstHalf':
                t['fouls_first_half']  += 1
                if p: p['fouls_first_half'] += 1
            else:
                t['fouls_second_half'] += 1
                if p: p['fouls_second_half'] += 1
            if _is_late(e):
                t['fouls_late_game'] += 1
                if p: p['fouls_late_game'] += 1
            # Type
            if _has_qid(e, 285):
                t['fouls_defensive'] += 1
                if p: p['fouls_defensive'] += 1
            elif _has_qid(e, 286):
                t['fouls_offensive'] += 1
                if p: p['fouls_offensive'] += 1
            if _has_qid(e, 264):
                t['fouls_aerial'] += 1
                if p: p['fouls_aerial'] += 1
        else:
            t['fouls_won']   += 1
            if p: p['fouls_won'] += 1

    # Card events
    for c in card_rows:
        tid = c['team_id']
        pid = c['player_id']
        key = (tid, pid)
        t   = team_stats[tid]

        if c['card_type'] == 'Yellow':
            t['yellow_cards']       += 1
        elif c['card_type'] == 'Red':
            t['red_cards']          += 1
        elif c['card_type'] == 'SecondYellow':
            t['second_yellow_cards'] += 1
            t['red_cards']          += 1   # second yellow = effective red

        if key in player_stats:
            p = player_stats[key]
            if c['card_type'] == 'Yellow':       p['yellow_cards']        += 1
            elif c['card_type'] == 'Red':        p['red_cards']           += 1
            elif c['card_type'] == 'SecondYellow':
                p['second_yellow_cards'] = p.get('second_yellow_cards', 0) + 1
                p['red_cards']           += 1

    # Offside events
    for e in events:
        etype = e['type']['displayName']
        tid   = e.get('teamId')
        if tid not in (home_id, away_id): continue
        pid   = e.get('playerId')
        t     = team_stats[tid]

        if etype == 'OffsideGiven':
            # Player from THIS team was caught offside
            t['offsides_committed'] += 1
            key = (tid, pid)
            if key in player_stats: player_stats[key]['offsides_committed'] += 1

        elif etype == 'OffsidePass':
            # Supplementary event — the pass that led to an offside.
            # OffsideGiven (above) already counts the offside for the team.
            # We only extract the GoalDisallowed qualifier here.
            if _has_qid(e, 8):
                t['goals_disallowed_offside'] += 1

        elif etype == 'OffsideProvoked':
            t['offsides_caught'] += 1

    # ── Derived team metrics ───────────────────────────────────────────────────
    for tid, t in team_stats.items():
        fc = t['fouls_committed']
        t['dangerous_foul_rate_pct'] = (
            round((t['fouls_own_box'] + t['fouls_danger_area']) / fc * 100, 2)
            if fc else None
        )
        t['foul_escalation_rate'] = (
            round(t['fouls_second_half'] / t['fouls_first_half'], 2)
            if t['fouls_first_half'] else None
        )
        total_cards = t['yellow_cards'] + t['red_cards']
        t['foul_to_card_ratio'] = (
            round(fc / total_cards, 1) if total_cards else None
        )

    # ── Finalise ───────────────────────────────────────────────────────────────
    team_rows   = [team_stats[tid] for tid in (home_id, away_id)]
    player_rows = list(player_stats.values())

    return {
        'team':   team_rows,
        'player': player_rows,
        'cards':  card_rows,
    }

if __name__ == "__main__":
    import json
    # path = './1903468.json'
    path = './whoscored_data/new_sun.json'
    with open(path, "r", encoding='utf-8') as file:
        file = json.load(file)

    result = parse_discipline(file, 1729476)
    # result = get_pro_carries(path)
    with open("./result.json", "w", encoding='utf-8') as fp:
        json.dump(result , fp, indent=4)