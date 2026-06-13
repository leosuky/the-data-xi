"""
parse_possession.py
-------------------
Parses WhoScored matchCentreData into core possession statistics.

DATA SOURCES
------------
isTouch=True flag    — on every event; used for touch counts and heatmap coordinates
Team stats dict      — possession (per-minute values, ratio-based %)
                     — touches (per-minute totals)
                     — dribblesAttempted / dribblesWon / dribblesLost / dribbleSuccess
                     — dispossessed / ballRecovery
TakeOn events        — dribble attempts; QID 286=Offensive, 285=Defensive, 211=OverRun
Dispossessed events  — possession losses; QID 286=Offensive, 285=Defensive
BallRecovery events  — ball wins
ShieldBallOpp events — holding ball under pressure
CornerAwarded events — corners won by x > 50 (near opponent goal line).
                       Each corner incident fires TWO events (one per team paired via OppRelEv);
                       x > 50 on a team's event = they won the corner. ✅ validated.

POSSESSION %
------------
WS stores per-minute possession values in stats['possession'].
Possession % = team_sum / (home_sum + away_sum). Verified exact (72.5/27.5, 58.9/41.1) ✅

PASSING RATE
------------
Accurate passes / possession minutes (= possession% × 90).
Measures tempo of play — how many successful passes per minute of actual possession.
Key predictor of goalscoring identified in possession research literature.

BALL RETENTION %
----------------
Press-resistance / ball security: of all the touches a player (or team) took,
what share did NOT end in losing the ball while in possession.

    ball_losses        = dispossessed + dribbles_lost
    ball_retention_pct = (touches - ball_losses) / touches * 100

"Losses" here are on-ball losses only — being dispossessed under pressure and
failed take-ons. Both are themselves touches (isTouch=True in WhoScored), so
ball_losses <= touches and the percentage is always in [0, 100]. Misplaced
passes are deliberately EXCLUDED: a pass is an intentional release of the ball,
tracked (with its accuracy) in parse_passing. Join the two parsers if you want
a full turnover model that also weighs giveaways from passing.

TOUCH ZONES
-----------
Own third:    x ≤ 33.3
Middle third: 33.3 < x ≤ 66.6
Final third:  x > 66.6
Penalty area: x > 83 AND 21 ≤ y ≤ 79

CORNER ATTRIBUTION
------------------
corners_won = CornerAwarded events for this team where x > 50.
MissLeft (QID 73) = corner from attacking right / defending left flank.
MissRight (QID 75) = corner from attacking left / defending right flank.
"""

from collections import defaultdict


# ── Constants ─────────────────────────────────────────────────────────────────

OWN_THIRD_MAX   = 33.3
FINAL_THIRD_MIN = 66.6
BOX_X_MIN       = 83.0
BOX_Y_MIN       = 21.0
BOX_Y_MAX       = 79.0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _has_qid(event, qid: int) -> bool:
    return any(q.get('type', {}).get('value') == qid
               for q in event.get('qualifiers', []))

def _acc(event) -> bool:
    return event.get('outcomeType', {}).get('displayName') == 'Successful'

def _flt(v, default=0.0) -> float:
    try: return float(v)
    except: return default

def _touch_zone(x: float) -> str:
    if x <= OWN_THIRD_MAX:   return 'own_third'
    if x <= FINAL_THIRD_MIN: return 'middle_third'
    return 'final_third'

def _in_box(x: float, y: float) -> bool:
    return x > BOX_X_MIN and BOX_Y_MIN <= y <= BOX_Y_MAX

def _pinit(player_acc: dict, tid: int, pid: int, names: dict) -> dict:
    """Get-or-create a player accumulator entry (all counters zeroed).

    Used by every per-player branch so dribbles/dispossessed accumulate safely
    regardless of event order or whether a touch was seen first.
    """
    key = (tid, pid)
    if key not in player_acc:
        player_acc[key] = {
            'team_id': tid, 'player_id': pid,
            'player_name': names.get(str(pid)),
            'touches': 0, 'touches_own_third': 0,
            'touches_middle_third': 0, 'touches_final_third': 0,
            'touches_in_box': 0, 'touch_x_sum': 0.0,
            'dribbles_attempted': 0, 'dribbles_won': 0, 'dribbles_lost': 0,
            'dribbles_own_third': 0, 'dribbles_middle_third': 0,
            'dribbles_final_third': 0,
            'dribbles_won_own_third': 0, 'dribbles_won_middle_third': 0,
            'dribbles_won_final_third': 0,
            'dispossessed': 0,
        }
    return player_acc[key]

# Qualifier IDs excluded from base pass count (mirrors parse_passing baseline)
# QID 2=Cross/CornerTaken, QID 107=ThrowIn, QID 123=KeeperThrow
_PASS_EXCL = {2, 107, 123}


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_possession(data: dict, whoscored_match_id: int) -> dict:
    """
    Parse core possession statistics from WhoScored matchCentreData.

    Returns:
        {
            'team':   [home_row, away_row]
            'player': [one row per player with possession involvement]
        }
    """
    events  = data.get('events', [])
    home_id = data['home']['teamId']
    away_id = data['away']['teamId']
    names   = data.get('playerIdNameDictionary', {})

    # ── Possession % from stats ────────────────────────────────────────────────
    home_poss_sum = sum(_flt(v) for v in data['home']['stats'].get('possession', {}).values())
    away_poss_sum = sum(_flt(v) for v in data['away']['stats'].get('possession', {}).values())
    total_poss    = home_poss_sum + away_poss_sum

    poss_pct = {
        home_id: round(home_poss_sum / total_poss * 100, 1) if total_poss else 50.0,
        away_id: round(away_poss_sum / total_poss * 100, 1) if total_poss else 50.0,
    }
    poss_minutes = {
        home_id: round(poss_pct[home_id] / 100 * 90, 2),
        away_id: round(poss_pct[away_id] / 100 * 90, 2),
    }

    # ── Accumulate team and player stats ──────────────────────────────────────
    team_acc   = {}
    player_acc = {}

    for tid in (home_id, away_id):
        team_acc[tid] = {
            # Touches
            'touches_total':         0,
            'touches_own_third':     0,
            'touches_middle_third':  0,
            'touches_final_third':   0,
            'touches_in_box':        0,
            'touch_x_sum':           0.0,
            # Dribbles
            'dribbles_attempted':    0,
            'dribbles_won':          0,
            'dribbles_lost':         0,
            'dribbles_offensive':    0,
            'dribbles_defensive':    0,
            'dribbles_overrun':      0,
            # Dribbles by zone (attempted / won)
            'dribbles_own_third':    0,
            'dribbles_middle_third': 0,
            'dribbles_final_third':  0,
            'dribbles_won_own_third':    0,
            'dribbles_won_middle_third': 0,
            'dribbles_won_final_third':  0,
            # Retention
            'dispossessed':          0,
            'dispossessed_offensive':0,
            'dispossessed_defensive':0,
            'shield_ball_opp':       0,
            # Base passes for passing rate (excl QIDs 2/107/123 — same as parse_passing baseline)
            '_base_passes_acc':      0,
            # Corners
            'corners_won':           0,
            'corners_miss_left':     0,
            'corners_miss_right':    0,
        }

    for e in events:
        tid = e.get('teamId')
        if tid not in (home_id, away_id): continue
        pid   = e.get('playerId')
        etype = e['type']['displayName']
        x     = _flt(e.get('x', 50))
        y     = _flt(e.get('y', 50))
        t     = team_acc[tid]

        # ── Touches (isTouch=True) ─────────────────────────────────────────────
        if e.get('isTouch'):
            t['touches_total']    += 1
            t['touch_x_sum']      += x
            zone = _touch_zone(x)
            t[f'touches_{zone}']  += 1
            if _in_box(x, y):
                t['touches_in_box'] += 1

            # Player-level touch accumulation
            if pid:
                p = _pinit(player_acc, tid, pid, names)
                p['touches']         += 1
                p['touch_x_sum']     += x
                p[f'touches_{zone}'] += 1
                if _in_box(x, y): p['touches_in_box'] += 1

        # ── TakeOn (dribbles) ──────────────────────────────────────────────────
        # WhoScored "dribbles" in stats = offensive TakeOns (QID 286).
        # Defensive TakeOns (QID 285) = shielding ball, tracked separately.
        # We report both totals but dribbles_attempted aligns with WS offensive definition.
        if etype == 'TakeOn':
            is_off  = _has_qid(e, 286)
            is_def  = _has_qid(e, 285)
            is_won  = _acc(e)
            zone    = _touch_zone(x)
            t['dribbles_attempted'] += 1          # all TakeOns
            t[f'dribbles_{zone}']   += 1
            if is_off:  t['dribbles_offensive'] += 1
            if is_def:  t['dribbles_defensive'] += 1
            if _has_qid(e, 211): t['dribbles_overrun'] += 1
            if is_won:
                t['dribbles_won']             += 1
                t[f'dribbles_won_{zone}']     += 1
            else:
                t['dribbles_lost'] += 1
            if pid:
                p = _pinit(player_acc, tid, pid, names)
                p['dribbles_attempted'] += 1
                p[f'dribbles_{zone}']   += 1
                if is_won:
                    p['dribbles_won']         += 1
                    p[f'dribbles_won_{zone}'] += 1
                else:
                    p['dribbles_lost']        += 1

        # ── Dispossessed ───────────────────────────────────────────────────────
        elif etype == 'Dispossessed':
            t['dispossessed'] += 1
            if _has_qid(e, 286): t['dispossessed_offensive'] += 1
            if _has_qid(e, 285): t['dispossessed_defensive'] += 1
            if pid:
                _pinit(player_acc, tid, pid, names)['dispossessed'] += 1

        # ── Ball recovery — already in parse_defending, not repeated here ────
        # ── Shield ────────────────────────────────────────────────────────────
        elif etype == 'ShieldBallOpp':
            t['shield_ball_opp'] += 1

        # ── Base passes for passing rate ──────────────────────────────────────
        # Use same exclusions as parse_passing baseline: excl QIDs {2, 107, 123}
        # (Cross/CornerTaken, ThrowIn, KeeperThrow)
        # Raw counts are already in parse_passing — only the rate is stored here.
        elif etype == 'Pass':
            if not any(_has_qid(e, q) for q in _PASS_EXCL):
                if _acc(e):
                    t['_base_passes_acc'] += 1

        # ── Corners won ───────────────────────────────────────────────────────
        # Each corner incident fires two CornerAwarded events (one per team, paired).
        # The team that WON the corner has their event near the opponent's goal (x > 50).
        elif etype == 'CornerAwarded' and x > 50:
            t['corners_won']       += 1
            if _has_qid(e, 73): t['corners_miss_left']  += 1
            if _has_qid(e, 75): t['corners_miss_right'] += 1

    # ── Build team rows ────────────────────────────────────────────────────────
    team_rows = []
    for tid in (home_id, away_id):
        t   = team_acc[tid]
        pct = poss_pct[tid]
        pm  = poss_minutes[tid]

        n_touch = t['touches_total']
        avg_x   = round(t['touch_x_sum'] / n_touch, 1) if n_touch else None

        drib_att = t['dribbles_attempted']
        drib_pct = round(t['dribbles_won'] / drib_att * 100, 1) if drib_att else None

        # Passing rate = accurate base passes per possession minute
        # Base passes exclude crosses, throw-ins, keeper throws (same as parse_passing)
        passing_rate = round(t['_base_passes_acc'] / pm, 2) if pm else None

        team_rows.append({
            'whoscored_match_id':        whoscored_match_id,
            'team_id':                   tid,
            # Possession
            'possession_pct':            pct,
            'possession_minutes':        pm,
            # Touches
            'touches_total':             n_touch,
            'touches_own_third':         t['touches_own_third'],
            'touches_middle_third':      t['touches_middle_third'],
            'touches_final_third':       t['touches_final_third'],
            'touches_in_box':            t['touches_in_box'],
            'avg_touch_x':               avg_x,
            # Dribbles
            'dribbles_attempted':        drib_att,
            'dribbles_won':              t['dribbles_won'],
            'dribbles_lost':             t['dribbles_lost'],
            'dribble_success_pct':       drib_pct,
            'dribbles_offensive':        t['dribbles_offensive'],
            'dribbles_defensive':        t['dribbles_defensive'],
            'dribbles_overrun':          t['dribbles_overrun'],
            # Dribbles by zone (attempted / won / success pct)
            'dribbles_own_third':        t['dribbles_own_third'],
            'dribbles_middle_third':     t['dribbles_middle_third'],
            'dribbles_final_third':      t['dribbles_final_third'],
            'dribbles_won_own_third':    t['dribbles_won_own_third'],
            'dribbles_won_middle_third': t['dribbles_won_middle_third'],
            'dribbles_won_final_third':  t['dribbles_won_final_third'],
            'dribble_success_pct_own_third':    (round(t['dribbles_won_own_third']    / t['dribbles_own_third']    * 100, 1) if t['dribbles_own_third']    else None),
            'dribble_success_pct_middle_third': (round(t['dribbles_won_middle_third'] / t['dribbles_middle_third'] * 100, 1) if t['dribbles_middle_third'] else None),
            'dribble_success_pct_final_third':  (round(t['dribbles_won_final_third']  / t['dribbles_final_third']  * 100, 1) if t['dribbles_final_third']  else None),
            # Retention
            'dispossessed':              t['dispossessed'],
            'dispossessed_offensive':    t['dispossessed_offensive'],
            'dispossessed_defensive':    t['dispossessed_defensive'],
            'shield_ball_opp':           t['shield_ball_opp'],
            # Ball retention % = (touches - on-ball losses) / touches.
            # On-ball losses = dispossessed + failed take-ons (see module docstring).
            'ball_losses':               t['dispossessed'] + t['dribbles_lost'],
            'ball_retention_pct':        (round((n_touch - (t['dispossessed'] + t['dribbles_lost'])) / n_touch * 100, 1)
                                          if n_touch else None),
            # Passing rate (derived — raw counts are in parse_passing)
            'passing_rate':              passing_rate,  # acc base passes / possession minute
            # Corners
            'corners_won':               t['corners_won'],
            'corners_miss_left':         t['corners_miss_left'],
            'corners_miss_right':        t['corners_miss_right'],
        })

    # ── Build player rows ──────────────────────────────────────────────────────
    player_rows = []
    for (tid, pid), p in player_acc.items():
        n_touch = p['touches']
        avg_x   = round(p['touch_x_sum'] / n_touch, 1) if n_touch else None
        d_att   = p['dribbles_attempted']
        d_pct   = round(p['dribbles_won'] / d_att * 100, 1) if d_att else None
        def _zpct(z):
            att = p[f'dribbles_{z}']
            return round(p[f'dribbles_won_{z}'] / att * 100, 1) if att else None
        player_rows.append({
            'whoscored_match_id':     whoscored_match_id,
            'team_id':                p['team_id'],
            'player_id':              pid,
            'player_name':            p['player_name'],
            'touches':                n_touch,
            'touches_own_third':      p['touches_own_third'],
            'touches_middle_third':   p['touches_middle_third'],
            'touches_final_third':    p['touches_final_third'],
            'touches_in_box':         p['touches_in_box'],
            'avg_touch_x':            avg_x,
            'dribbles_attempted':     d_att,
            'dribbles_won':           p['dribbles_won'],
            'dribbles_lost':          p['dribbles_lost'],
            'dribble_success_pct':    d_pct,
            'dribbles_own_third':         p['dribbles_own_third'],
            'dribbles_middle_third':      p['dribbles_middle_third'],
            'dribbles_final_third':       p['dribbles_final_third'],
            'dribbles_won_own_third':     p['dribbles_won_own_third'],
            'dribbles_won_middle_third':  p['dribbles_won_middle_third'],
            'dribbles_won_final_third':   p['dribbles_won_final_third'],
            'dribble_success_pct_own_third':    _zpct('own_third'),
            'dribble_success_pct_middle_third': _zpct('middle_third'),
            'dribble_success_pct_final_third':  _zpct('final_third'),
            'dispossessed':           p['dispossessed'],
            # Ball retention % — on-ball losses = dispossessed + failed take-ons
            'ball_losses':            p['dispossessed'] + p['dribbles_lost'],
            'ball_retention_pct':     (round((n_touch - (p['dispossessed'] + p['dribbles_lost'])) / n_touch * 100, 1)
                                       if n_touch else None),
        })

    return {
        'team':   team_rows,
        'player': sorted(player_rows, key=lambda r: -r['touches']),
    }


if __name__ == "__main__":
    import json
    path = './1903468.json'
    # path = './whoscored_data/new_sun.json'
    with open(path, "r", encoding='utf-8') as file:
        file = json.load(file)

    result = parse_possession(file, 1729476)
    with open("./result.json", "w", encoding='utf-8') as fp:
        json.dump(result , fp, indent=4)