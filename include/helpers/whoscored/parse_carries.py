"""
parse_carries.py  (v2 — possession-chain method)
------------------------------------------------
Derives carry metrics from WhoScored matchCentreData events.

HOW CARRIES ARE DERIVED
-----------------------
v1 used a backwards lookback from each on-ball event. That produced two
systematic errors:
  1. PHANTOM CARRIES — a player's previous on-ball event was treated as a
     receipt even when they had released the ball in between (pass -> sprint
     30m -> tackle was counted as a 30m carry). This inflated distance ~+19%.
  2. MISSED CARRIES — the 4-event lookback broke whenever duel events
     (Challenge, Aerial pairs) were interleaved mid-dribble, and receipts
     from loose balls (failed clearances, deflections) were never anchored.

v2 walks the match chronologically with a ball-state machine:

  anchor  = last known ball position (+ which team's coordinate frame it
            is expressed in — WhoScored coords are team-relative)
  holder  = player in controlled possession (None = ball loose / in transit)

A carry is recorded when the holder (or the collector of a loose/in-transit
ball) performs their next controlled on-ball action: receipt point -> action
point, straight-line, on a 105 x 68 m pitch.

Chain rules (the important ones):
  - Successful pass        -> ball in transit to (endX, endY); the receiving
                              teammate's first controlled action anchors the carry.
  - Failed Challenge       -> NO-OP (a defender being dribbled past must not
                              break the carrier's chain).
  - Aerial loser           -> NO-OP. Aerial winner -> takes possession at the
                              duel position.
  - Tackle/Interception/
    Recovery/KeeperPickup  -> the gainer takes possession at the event position
                              (these are first touches; they can START a carry,
                              never END one).
  - Clearance              -> ball loose at the clearance endpoint (collectible
                              by either team, coordinates flipped across frames).
  - Unsuccessful BallTouch -> ball loose at that position.
  - Foul / offside / card /
    sub / period boundary  -> dead ball, chain killed.
  - Set-piece restarts     -> an event with a ThrowIn / FreekickTaken /
    (the v1 distance bug)     CornerTaken / GoalKick / KickOff qualifier never
                              ends a carry: the player WALKED to the restart
                              spot, the ball did not travel with them.
  - A player fouled while in possession ends (and records) their carry at the
    foul spot.

Parameters (calibrated vs Sofascore across 4 PL matches, 117 player rows:
Villa-West Ham, Spurs-Forest, Newcastle-Sunderland, Everton-Chelsea):
  MIN_CARRY_DIST = 3.25 m, MAX_TIME_GAP = 8 s
  4-match validation vs Sofascore:
    Count:    r = 0.91-0.99 per match, MAE = 1.42, team totals within ~3%
    Distance: r = 0.92-0.98, total error 0.0% to +3.3% per match
    Progressive: MAE = 0.24, match totals within +-4
  GK rule: KeeperPickup / Save give the keeper possession but IN HANDS —
  movement before release is not a ball carry and is never counted.
  Remaining per-player scatter (~+-1.4 carries) is largely irreducible:
  when WhoScored logs no recovery/first-touch event for a loose-ball pickup,
  the receipt point Opta used simply is not in the feed. Verified dead ends:
  anchoring failed-pass endpoints (+20% distance), soft same-team transfers
  from a holder, lower/higher thresholds, longer time gaps.

PROGRESSIVE CARRY DEFINITION
----------------------------
A carry that moves the ball at least 25% closer to the centre of the
opponent's goal (the same relative rule Opta uses for progressive passes —
empirically this is what Sofascore's progressiveBallCarriesCount matches,
NOT the published "5m upfield" Opta carry rule, which over-fires ~5x).

FINAL THIRD ENTRY VIA CARRY
---------------------------
A carry that starts outside the final third (start_x < 66.6) and ends inside
it (end_x >= 66.6). Supplements parse_passing's final-third entries.

UNITS
-----
All distances in metres (pitch scaled to 105m x 68m from WhoScored 0-100).
"""

import math


# ── Constants ─────────────────────────────────────────────────────────────────

PITCH_LENGTH   = 105.0
PITCH_WIDTH    = 68.0
MIN_CARRY_DIST = 3.25    # metres
MAX_TIME_GAP   = 8       # seconds between receipt and action
PROG_PCT       = 0.25    # progressive = >=25% closer to opponent goal
FINAL_THIRD_X  = 66.6

DEAD_BALL_QUALIFIERS = {'ThrowIn', 'CornerTaken', 'FreekickTaken',
                        'GoalKick', 'KickOff', 'PenaltyTaken'}

# Possession-gaining first touches AT FEET: can start a chain, never end a carry.
GAIN_TYPES = {'BallRecovery', 'Interception', 'Tackle'}

# GK gains IN HANDS: the keeper has possession, but movement before release is
# carrying the ball in the hands — Opta/Sofascore do not count it as a ball
# carry (validated: Robert Sanchez 9 -> 4 phantom carries removed).
GAIN_HANDS_TYPES = {'KeeperPickup', 'Save'}

# Controlled on-ball actions: these END (record) a carry.
CONTROLLED_ACTIONS = {'Pass', 'TakeOn', 'MissedShots', 'SavedShot', 'Goal',
                      'ShotOnPost', 'Dispossessed', 'Clearance', 'BallTouch',
                      'OffsidePass', 'KeeperSweeper'}

# Hard stoppages: dead ball, chain killed.
STOPPAGE_TYPES = {'End', 'Start', 'OffsideGiven', 'OffsideProvoked',
                  'CornerAwarded', 'Card', 'SubstitutionOn', 'SubstitutionOff',
                  'FormationChange', 'FormationSet'}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dist_m(x1, y1, x2, y2):
    return math.sqrt(((x2 - x1) * PITCH_LENGTH / 100) ** 2
                     + ((y2 - y1) * PITCH_WIDTH / 100) ** 2)


def _goal_dist_m(x, y):
    """Distance to the centre of the opponent's goal (x=100, y=50)."""
    return math.sqrt(((100 - x) * PITCH_LENGTH / 100) ** 2
                     + ((50 - y) * PITCH_WIDTH / 100) ** 2)


def _get_time(event):
    return event.get('expandedMinute', 0) * 60 + (event.get('second') or 0)


def _qualifiers(event):
    return {q.get('type', {}).get('displayName') for q in event.get('qualifiers', [])}


# ── Carry derivation ──────────────────────────────────────────────────────────

def _derive_carries(sorted_events, home_id, away_id):
    """
    Ball-state machine. Returns
    {player_id: {team_id, carries, carry_distance_m,
                 progressive_carries, carry_final_third_entries}}
    """
    # Ball state ------------------------------------------------------------
    ax = ay = None      # anchor position (last known ball location)
    a_team = None       # coordinate frame the anchor is expressed in
    a_time = None
    holder = None       # player in controlled possession; None = loose
    in_hands = False    # holder is a GK holding the ball in hands

    data = {}

    def clear():
        nonlocal ax, ay, a_team, a_time, holder, in_hands
        ax = ay = a_team = a_time = holder = None
        in_hands = False

    def set_anchor(x, y, team, pid, now, hands=False):
        nonlocal ax, ay, a_team, a_time, holder, in_hands
        ax, ay, a_team, a_time, holder = x, y, team, now, pid
        in_hands = hands

    def record(pid, team, end_x, end_y):
        # anchor may be in the other team's frame: flip if needed
        sx, sy = (ax, ay) if a_team == team else (100 - ax, 100 - ay)
        d = _dist_m(sx, sy, end_x, end_y)
        if d < MIN_CARRY_DIST:
            return
        row = data.setdefault(pid, {
            'team_id': team, 'carries': 0, 'carry_distance_m': 0.0,
            'progressive_carries': 0, 'carry_final_third_entries': 0,
        })
        row['carries'] += 1
        row['carry_distance_m'] += d
        g0, g1 = _goal_dist_m(sx, sy), _goal_dist_m(end_x, end_y)
        if g0 > 0 and (g0 - g1) / g0 >= PROG_PCT:
            row['progressive_carries'] += 1
        if sx < FINAL_THIRD_X <= end_x:
            row['carry_final_third_entries'] += 1

    # Main loop ---------------------------------------------------------------
    for e in sorted_events:
        etype = e.get('type', {}).get('displayName')

        if etype in STOPPAGE_TYPES:
            clear()
            continue
        if etype == 'Challenge':            # failed challenge on the carrier
            continue

        pid, team = e.get('playerId'), e.get('teamId')
        x, y = e.get('x'), e.get('y')
        if pid is None or x is None or y is None or team not in (home_id, away_id):
            continue

        outcome = e.get('outcomeType', {}).get('displayName')
        q = _qualifiers(e)
        now = _get_time(e)
        dead_restart = bool(q & DEAD_BALL_QUALIFIERS)
        time_ok = a_time is None or (now - a_time) <= MAX_TIME_GAP

        # ── 1. Record a carry ending at this event ─────────────────────────
        fouled_in_possession = (etype == 'Foul' and outcome == 'Successful'
                                and holder == pid)
        if ax is not None and not dead_restart and time_ok and not in_hands:
            if etype in CONTROLLED_ACTIONS or fouled_in_possession:
                if holder == pid:
                    record(pid, team, x, y)
                elif holder is None and a_team == team:
                    # collecting a teammate's pass / loose ball in own frame
                    record(pid, team, x, y)
                elif holder is None and a_team != team:
                    # loose ball anchored in opponent frame (e.g. their
                    # clearance endpoint) — flip handled inside record()
                    record(pid, team, x, y)

        # ── 2. Update ball state ───────────────────────────────────────────
        if etype == 'Pass':
            end_x, end_y = e.get('endX'), e.get('endY')
            if outcome == 'Successful' and end_x is not None:
                set_anchor(end_x, end_y, team, None, now)   # in transit
            else:
                clear()                                     # turnover/out
        elif etype == 'Clearance':
            end_x, end_y = e.get('endX'), e.get('endY')
            if end_x is not None:
                set_anchor(end_x, end_y, team, None, now)   # loose at endpoint
            else:
                clear()
        elif etype in GAIN_TYPES:
            set_anchor(x, y, team, pid, now)
        elif etype in GAIN_HANDS_TYPES:
            set_anchor(x, y, team, pid, now, hands=True)
        elif etype == 'TakeOn':
            if outcome == 'Successful':
                set_anchor(x, y, team, pid, now)
            else:
                clear()
        elif etype == 'BallTouch':
            if outcome == 'Successful':
                set_anchor(x, y, team, pid, now)
            else:
                set_anchor(x, y, team, None, now)           # loose deflection
        elif etype == 'Aerial':
            if outcome == 'Successful':
                set_anchor(x, y, team, pid, now)            # winner has it
            # loser: NO-OP
        elif etype == 'BlockedPass':
            set_anchor(x, y, team, None, now)
        elif etype in ('Foul', 'Dispossessed', 'MissedShots', 'Goal',
                       'ShotOnPost', 'SavedShot', 'Punch', 'KeeperSweeper',
                       'OffsidePass'):
            clear()

    return data


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_carries(data: dict, whoscored_match_id: int) -> dict:
    """
    Derive carry statistics from WhoScored matchCentreData.

    Returns:
        {
            'player': [one row per player who had at least one carry],
            'team':   [one row per team — aggregated from player rows]
        }
    """
    events  = data.get('events', [])
    home_id = data['home']['teamId']
    away_id = data['away']['teamId']
    names   = data.get('playerIdNameDictionary', {})

    sorted_events = sorted(events, key=lambda e: (_get_time(e), e.get('eventId', 0)))

    raw = _derive_carries(sorted_events, home_id, away_id)

    player_rows = []
    for p_id, cd in raw.items():
        player_rows.append({
            'whoscored_match_id':        whoscored_match_id,
            'team_id':                   cd['team_id'],
            'player_id':                 p_id,
            'player_name':               names.get(str(p_id)),
            'carries':                   cd['carries'],
            'carry_distance_m':          round(cd['carry_distance_m'], 1),
            'progressive_carries':       cd['progressive_carries'],
            'carry_final_third_entries': cd['carry_final_third_entries'],
        })

    team_agg = {}
    for row in player_rows:
        tid = row['team_id']
        agg = team_agg.setdefault(tid, {
            'whoscored_match_id': whoscored_match_id, 'team_id': tid,
            'carries': 0, 'carry_distance_m': 0.0,
            'progressive_carries': 0, 'carry_final_third_entries': 0,
        })
        agg['carries']                   += row['carries']
        agg['carry_distance_m']          += row['carry_distance_m']
        agg['progressive_carries']       += row['progressive_carries']
        agg['carry_final_third_entries'] += row['carry_final_third_entries']

    team_rows = []
    for tid in (home_id, away_id):
        if tid in team_agg:
            r = team_agg[tid]
            r['carry_distance_m'] = round(r['carry_distance_m'], 1)
            team_rows.append(r)

    return {
        'player': sorted(player_rows, key=lambda r: -r['carries']),
        'team':   team_rows,
    }


if __name__ == "__main__":
    import json
    path = './1903468.json'
    with open(path, "r", encoding='utf-8') as fp:
        match_data = json.load(fp)

    result = parse_carries(match_data, 1729476)
    with open("./result.json", "w", encoding='utf-8') as fp:
        json.dump(result, fp, indent=4)
