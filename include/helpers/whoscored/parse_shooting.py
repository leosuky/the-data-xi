"""
parse_shooting.py
-----------------
Parses WhoScored matchCentreData events into shooting stats.

SHOT CLASSIFICATION
-------------------
WhoScored marks all shot attempts with isShot=True on four event types:
    Goal        (type 16)
    SavedShot   (type 15)
    ShotOnPost  (type 14)  — hit the woodwork; a shot, counted OFF TARGET
    MissedShots (type 13)

BlockedPass events (type 74) are the DEFENDER's perspective —
they never carry isShot=True and are counted separately in parse_passing.py.

OUTCOME DEFINITIONS
-------------------
Total shots      = isShot=True events
Shots on target  = Goal + SavedShot WITHOUT Blocked qualifier (QID 82)
Blocked shots    = SavedShot WITH Blocked qualifier (QID 82)
                   An outfield player blocked it; keeper was not beaten.
Shots off target = MissedShots + ShotOnPost
                   Shots hitting the woodwork are off target per Opta.

Verified exact on 4 teams across 2 games vs Fotmob / Sofascore.

ADVANCED METRICS
----------------
xGOT (Fotmob/Opta PSxG) is joined later via combo_id at the pipeline level.
This parser produces per-shot rows ready for that join.

WhoScored PSxG (derived from GoalMouthY/Z placement) is computed here
as a per-shot alternative when Fotmob data is unavailable.

UNITS
-----
Distance: metres (pitch scaled to 105m × 68m from 0-100 coordinates)
Angle: degrees (goal subtension angle from shot position)
"""

import math
from collections import defaultdict


# ── Constants ─────────────────────────────────────────────────────────────────

# Shot event type IDs. ShotOnPost (14) carries isShot=True and IS a shot
# (Sofascore/Opta/FBref all count shots off the woodwork) — it is OFF TARGET
# (the post stopped it; no goal, no save). Verified: Newcastle-Sunderland is
# 33 shots incl. one ShotOnPost, matching Sofascore's 16+17.
SHOT_TYPES = {16, 15, 14, 13}   # Goal, SavedShot, ShotOnPost, MissedShots

# Blocked qualifier — SavedShot WITH this = blocked shot (not SoT)
QID_BLOCKED = 82

# Set-piece pattern qualifiers on shots
# These are different from the pass dead ball QIDs used in parse_passing.py
# QID 22=RegularPlay, QID 23=FastBreak, QID 24=SetPiece,
# QID 25=FromCorner, QID 160=ThrowinSetPiece
# FreekickTaken (QID 5) also appears on some shots
SHOT_SETPIECE_QS = {'SetPiece', 'FromCorner', 'FreekickTaken', 'ThrowinSetPiece'}

# In-box location qualifiers
IN_BOX_QS = {
    'BoxCentre', 'BoxLeft', 'BoxRight',
    'SmallBoxCentre', 'SmallBoxLeft', 'SmallBoxRight',
    'SixYardBlock', 'DeepBoxLeft', 'DeepBoxRight',
}

# Goal placement mappings
TOP_CORNER_QS    = {'HighLeft', 'HighRight'}
LOW_CORNER_QS    = {'LowLeft', 'LowRight'}
CENTRE_FRAME_QS  = {'LowCentre', 'HighCentre'}
MISS_QS          = {'MissHigh', 'MissLeft', 'MissRight', 'MissLow'}

# Pattern of play qualifiers on shots
PATTERN_QS = {
    'RegularPlay', 'FromCorner', 'SetPiece',
    'FreekickTaken', 'FastBreak', 'ThrowinSetPiece',
}

# ── Shot-creating action (SCA) vocabulary ─────────────────────────────────────
# FBref SCA = the (up to) two offensive actions by the shooting team directly
# preceding a shot. Validated vs FBref across 2 matches (Parma-Lazio,
# Juventus-Napoli 2018/19), 48 shots:
#   SCA1 105/105 players, SCA2 105/105 players, event-type classification
#   exact on all matched players. Validated across 4 matches (Parma-Lazio,
#   Juventus-Napoli, Napoli-Sassuolo, Como-Inter), 105 shots.
#
# Two possession rules make the walk-back match FBref exactly:
#   1. A reactive opponent touch (Clearance, BlockedPass, failed tackle, lost
#      aerial, fleeting deflection) that drops straight back to the attacking
#      team is TRANSPARENT — it does not reset shot creation, exactly as a
#      saved shot rebounding to another shot leaves the first shot an SCA.
#      A deliberate possession win (opponent completed pass / take-on / shot,
#      or an Interception) DOES end the chain.
#   2. A BallRecovery is transparent when the same team's ball-winning Tackle
#      or Interception sits immediately before it — that defensive action is
#      the creating action FBref credits.
#
# Ordering: WhoScored's delivered event order is used as-is. `eventId` is NOT
# unique within a match and `id` is scrambled in some archived feeds, so
# neither is used to re-sort; shots are located by object identity.
#
# Dead-ball pass qualifiers → 'Pass (Dead)', else 'Pass (Live)'.
SCA_DEAD_BALL_PASS = {'ThrowIn', 'CornerTaken', 'FreekickTaken', 'GoalKick',
                      'KickOff', 'IndirectFreekickTaken'}
# Non-creditable own-team events that START/interrupt a possession. When
# searching back for SCA2 we do not cross one of these (a recovery is where
# the attack began, not a creating action). Interception/Tackle ARE creditable
# (defensive-action SCA) so are deliberately excluded from this set.
SCA_POSSESSION_START = {'BallRecovery', 'KeeperPickup', 'Save', 'Clearance',
                        'BlockedPass'}
# Opponent on-ball actions that end the attacking chain when walking back.
SCA_BREAK_TYPES = {'Pass', 'TakeOn', 'Goal', 'SavedShot', 'MissedShots', 'ShotOnPost',
                   'BallRecovery', 'Interception', 'Tackle', 'Clearance'}


def _sca_time(e):
    return e.get('expandedMinute', 0) * 60 + (e.get('second') or 0)


def _sca_kind(e):
    """FBref-style SCA label for an event, or None if not creditable."""
    t = e['type']['displayName']
    if t == 'Pass':
        if not _acc(e):
            return None
        q = {qq['type']['displayName'] for qq in e.get('qualifiers', [])}
        return 'Pass (Dead)' if (q & SCA_DEAD_BALL_PASS) else 'Pass (Live)'
    if t == 'TakeOn':
        return 'Take-On' if _acc(e) else None
    if t in ('Goal', 'SavedShot', 'MissedShots', 'ShotOnPost'):
        return 'Shot'
    if t == 'Interception':
        return 'Interception'
    if t == 'Tackle':
        return 'Tackle'
    return None


def _resolve_sca(sorted_events, shot_pos, max_lookback=12):
    """
    Walk backwards from the shot at position `shot_pos` in `sorted_events`
    to find up to two shot-creating actions by the shooting team.

    The build-up runs SCA2 -> SCA1 -> Shot, so SCA1 is the action IMMEDIATELY
    before the shot and SCA2 is the one before SCA1. Walking backwards we meet
    SCA1 first, hence returned list is [SCA1, SCA2] (length 0-2).

    NB: WhoScored eventId is NOT unique within a match (it resets/repeats),
    so the shot must be located by POSITION in the sorted stream, never by
    eventId lookup.

    Rules (validated vs FBref):
      - Only the shooting team's offensive actions count.
      - A drawn Foul is logged on both players; the fouled (Successful) side
        is the creator and is credited when it belongs to the shooting team.
      - BallRecovery / keeper actions / loose touches are possession
        boundaries: SCA2 is not searched past the start of this possession.
      - An opponent on-ball action ends the chain.
      - If SCA1 is a rebound Shot, no SCA2 is credited.
    """
    shot = sorted_events[shot_pos]
    team = shot.get('teamId')
    found = []
    i = shot_pos - 1
    steps = 0
    while i >= 0 and steps < max_lookback and len(found) < 2:
        e = sorted_events[i]; steps += 1
        et = e['type']['displayName']
        eteam = e.get('teamId')

        if et == 'Foul':
            if eteam == team and _acc(e):
                found.append((e, 'Fouled'))
            i -= 1
            continue

        if eteam and eteam != team:
            # When walking back from the shot, the attacking team has the ball
            # at the shot. The chain ends only when the OPPONENT genuinely won /
            # controlled the ball. We distinguish:
            #
            #  BREAK (real possession change by the opponent):
            #   - completed opponent pass / successful take-on (they kept it)
            #   - opponent shot (they attacked)
            #   - Interception: the defender READ and stepped into a pass — a
            #     deliberate possession win, i.e. a genuine turnover. (Validated:
            #     Immobile's failed pass -> opp Interception -> rewin -> shot has
            #     no SCA per FBref.)
            #
            #  TRANSPARENT (reactive touch that fell back to the attack — does
            #  NOT reset shot creation, cf. a saved shot rebounding to a shot):
            #   - Clearance / BlockedPass: ball forced at the defender under
            #     pressure and dropped straight back to the attacking team.
            #   - failed tackles, lost aerials, fleeting deflecting touches.
            if et == 'Pass' and _acc(e):
                break
            if et == 'TakeOn' and _acc(e):
                break
            if et in ('Goal', 'SavedShot', 'MissedShots', 'ShotOnPost'):
                break
            if et == 'Interception':
                break
            # otherwise transparent — keep tracing back through it
            i -= 1
            continue

        if eteam == team:
            kind = _sca_kind(e)
            if kind is not None:
                found.append((e, kind))
                if len(found) == 1 and kind == 'Shot':
                    break
                i -= 1
                continue
            if et in SCA_POSSESSION_START:
                # A BallRecovery / loose-ball pickup is usually where the attack
                # began (not itself a creating action). But it is transparent
                # when the SAME team's ball-winning defensive action sits
                # immediately before it: that tackle/interception is the real
                # creating action (validated vs FBref: Pinamonti recovery <-
                # Lauriente tackle = SCA1; Štulac take-on/recovery <- Štulac
                # interception = SCA2). Peek one own-team step back.
                if et in ('BallRecovery',):
                    j = i - 1
                    hops = 0
                    while j >= 0 and hops < 3:
                        prev = sorted_events[j]
                        pteam = prev.get('teamId')
                        if pteam == team:
                            pk = _sca_kind(prev)
                            if pk in ('Tackle', 'Interception'):
                                found.append((prev, pk))
                            break
                        elif pteam and pteam != team:
                            break
                        j -= 1; hops += 1
                break
        i -= 1
    return found


# ── Helpers ───────────────────────────────────────────────────────────────────

def _has(event, name: str) -> bool:
    return any(q['type']['displayName'] == name
               for q in event.get('qualifiers', []))

def _has_qid(event, qid: int) -> bool:
    return any(q.get('type', {}).get('value') == qid
               for q in event.get('qualifiers', []))

def _get(event, name: str):
    for q in event.get('qualifiers', []):
        if q['type']['displayName'] == name:
            return q.get('value')
    return None

def _flt(v, default=0.0) -> float:
    try: return float(v)
    except: return default

def _acc(event) -> bool:
    return event.get('outcomeType', {}).get('displayName') == 'Successful'


def _is_blocked_shot(event) -> bool:
    """SavedShot with Blocked qualifier = outfield block = not SoT."""
    return (event['type']['displayName'] == 'SavedShot'
            and _has_qid(event, QID_BLOCKED))


def _is_sot(event) -> bool:
    """True if this shot is on target (goal or genuine save)."""
    t = event['type']['displayName']
    if t == 'Goal':
        return True
    if t == 'SavedShot' and not _has_qid(event, QID_BLOCKED):
        return True
    return False


def _is_open_play(event) -> bool:
    """
    A shot is open play if it carries no set-piece pattern qualifier.
    Note: shot events use pattern qualifiers (FromCorner, SetPiece, etc.)
    not the dead ball qualifier IDs used on pass events.
    """
    return not any(q['type']['displayName'] in SHOT_SETPIECE_QS
                   for q in event.get('qualifiers', []))


def _dist_to_goal(x: float, y: float) -> float:
    """
    Euclidean distance from (x, y) to centre of opponent goal (100, 50).
    Pitch scaled to 105m × 68m from 0-100 coordinates.
    """
    return round(math.sqrt(((100 - x) * 1.05) ** 2 + ((50 - y) * 0.68) ** 2), 2)


def _shot_angle(x: float, y: float, goal_width_m: float = 7.32) -> float:
    """
    Angle (degrees) subtended by the goal frame from shot position.
    Uses the simplified formula: 2 × arctan(goal_half / dist_to_goal).
    """
    dist = _dist_to_goal(x, y)
    if dist == 0:
        return 90.0
    return round(math.degrees(2 * math.atan((goal_width_m / 2) / dist)), 2)


def _body_part(event) -> str:
    for q in event.get('qualifiers', []):
        if q['type']['displayName'] in ('RightFoot', 'LeftFoot', 'Head', 'OtherBodyPart'):
            return q['type']['displayName']
    return 'Unknown'


def _location(event) -> str:
    """Primary shot location bucket from qualifier."""
    for q in event.get('qualifiers', []):
        name = q['type']['displayName']
        if name in IN_BOX_QS:
            return 'InBox'
        if name in ('OutOfBoxCentre', 'OutOfBoxLeft', 'OutOfBoxRight'):
            return 'OutOfBox'
    return 'Unknown'


def _pattern(event) -> str:
    for q in event.get('qualifiers', []):
        if q['type']['displayName'] in PATTERN_QS:
            return q['type']['displayName']
    return 'RegularPlay'


def _goalmouth_placement(event) -> str:
    """Categorical label for goal mouth placement."""
    for q in event.get('qualifiers', []):
        name = q['type']['displayName']
        if name in TOP_CORNER_QS:   return 'TopCorner'
        if name in LOW_CORNER_QS:   return 'LowCorner'
        if name in CENTRE_FRAME_QS: return 'CentreFrame'
        if name in MISS_QS:         return name   # MissHigh, MissLeft, MissRight
    return None


# ── Per-shot row ──────────────────────────────────────────────────────────────

def _shot_row(event, match_id: int, names: dict) -> dict:
    """Extract all fields from a single shot event."""
    x = _flt(event.get('x', 0))
    y = _flt(event.get('y', 50))
    etype = event['type']['displayName']

    is_goal    = etype == 'Goal'
    is_sot     = _is_sot(event)
    is_blocked = _is_blocked_shot(event)
    is_off     = etype in ('MissedShots', 'ShotOnPost')
    is_open    = _is_open_play(event)

    return {
        'whoscored_match_id': match_id,
        'event_id':           event.get('eventId'),
        'team_id':            event.get('teamId'),
        'player_id':          event.get('playerId'),
        'player_name':        names.get(str(event.get('playerId'))),
        'minute':             event.get('minute'),
        'second':             event.get('second'),
        'expanded_minute':    event.get('expandedMinute'),
        'period':             event.get('period', {}).get('displayName'),
        'x': x, 'y': y,
        # Outcome
        'is_goal':            is_goal,
        'is_on_target':       is_sot,
        'is_blocked':         is_blocked,
        'is_off_target':      is_off,
        'is_open_play':       is_open,
        # Location
        'location':           _location(event),
        'in_box':             _location(event) == 'InBox',
        'six_yard_box':       _has(event, 'SixYardBlock'),
        'small_box':          any(_has(event, q) for q in ('SmallBoxCentre','SmallBoxLeft','SmallBoxRight')),
        'distance_m':         _dist_to_goal(x, y),
        'angle_deg':          _shot_angle(x, y),
        # Body part & technique
        'body_part':          _body_part(event),
        'is_header':          _has(event, 'Head'),
        'is_right_foot':      _has(event, 'RightFoot'),
        'is_left_foot':       _has(event, 'LeftFoot'),
        'is_volley':          _has(event, 'Volley'),
        'is_first_touch':     _has(event, 'FirstTouch'),
        # Shot situation
        'pattern':            _pattern(event),
        'is_from_corner':     _has(event, 'FromCorner'),
        'is_from_freekick':   _has(event, 'FreekickTaken') or _has(event, 'SetPiece'),
        'is_fast_break':      _has(event, 'FastBreak'),
        'is_assisted':        _has_qid(event, 29),   # Assisted qualifier
        'is_individual_play': _has(event, 'IndividualPlay'),
        'is_big_chance':      _has(event, 'BigChance'),
        'is_one_on_one':      _has(event, 'OneOnOne'),
        # Goal mouth placement (only populated for SavedShot, Goal, some MissedShots)
        'goal_mouth_y':       _flt(_get(event, 'GoalMouthY'), None),
        'goal_mouth_z':       _flt(_get(event, 'GoalMouthZ'), None),
        'placement':          _goalmouth_placement(event),
        # Assist link
        'related_event_id':   _get(event, 'RelatedEventId'),
        # xGOT placeholder — joined from Fotmob via combo_id at pipeline level
        'xgot':               None,
    }


# ── Aggregation ───────────────────────────────────────────────────────────────

def _aggregate(rows: list, match_id: int, team_id, player_id, player_name) -> dict:
    """Aggregate per-shot rows into a stats dict."""
    if not rows:
        return None

    n = len(rows)

    goals        = sum(1 for r in rows if r['is_goal'])
    sot          = sum(1 for r in rows if r['is_on_target'])
    blocked      = sum(1 for r in rows if r['is_blocked'])
    off_target   = sum(1 for r in rows if r['is_off_target'])
    open_play    = [r for r in rows if r['is_open_play']]
    in_box       = [r for r in rows if r['in_box']]
    out_box      = [r for r in rows if not r['in_box']]

    # Accuracy & efficiency
    sot_pct      = round(sot / n * 100, 2) if n else 0
    conv_rate    = round(goals / n * 100, 2) if n else 0
    gpsot        = round(goals / sot * 100, 2) if sot else 0

    # Big chances
    big_ch       = sum(1 for r in rows if r['is_big_chance'])
    big_ch_goal  = sum(1 for r in rows if r['is_big_chance'] and r['is_goal'])
    big_ch_conv  = round(big_ch_goal / big_ch * 100, 2) if big_ch else None

    # Shot profile
    headers      = sum(1 for r in rows if r['is_header'])
    right_foot   = sum(1 for r in rows if r['is_right_foot'])
    left_foot    = sum(1 for r in rows if r['is_left_foot'])
    volleys      = sum(1 for r in rows if r['is_volley'])
    first_touch  = sum(1 for r in rows if r['is_first_touch'])

    # Location
    in_box_n     = len(in_box)
    in_box_goals = sum(1 for r in in_box if r['is_goal'])
    in_box_conv  = round(in_box_goals / in_box_n * 100, 2) if in_box_n else None
    out_box_n    = len(out_box)
    six_yard     = sum(1 for r in rows if r['six_yard_box'])

    # Distances
    dists = [r['distance_m'] for r in rows if r['distance_m'] is not None]
    avg_dist = round(sum(dists) / len(dists), 2) if dists else None

    # Patterns
    from_corner  = sum(1 for r in rows if r['is_from_corner'])
    from_fk      = sum(1 for r in rows if r['is_from_freekick'])
    fast_break   = sum(1 for r in rows if r['is_fast_break'])
    assisted     = sum(1 for r in rows if r['is_assisted'])
    individual   = sum(1 for r in rows if r['is_individual_play'])
    one_on_ones  = sum(1 for r in rows if r['is_one_on_one'])

    # Open play vs set piece
    op_shots     = len(open_play)
    op_goals     = sum(1 for r in open_play if r['is_goal'])
    op_sot       = sum(1 for r in open_play if r['is_on_target'])

    # Goal mouth placement distribution (on-target shots only)
    on_target = [r for r in rows if r['is_on_target']]
    top_corner  = sum(1 for r in on_target if r['placement'] == 'TopCorner')
    low_corner  = sum(1 for r in on_target if r['placement'] == 'LowCorner')
    centre_frame= sum(1 for r in on_target if r['placement'] == 'CentreFrame')

    return {
        'whoscored_match_id':        match_id,
        'team_id':                   team_id,
        'player_id':                 player_id,
        'player_name':               player_name,
        # 1. Volume
        'total_shots':               n,
        'goals':                     goals,
        'shots_on_target':           sot,
        'shots_blocked':             blocked,
        'shots_off_target':          off_target,
        'shots_on_target_pct':       sot_pct,
        'conversion_rate_pct':       conv_rate,
        'goals_per_shot_on_target_pct': gpsot,
        'big_chances':               big_ch,
        'big_chances_scored':        big_ch_goal,
        'big_chance_conversion_pct': big_ch_conv,
        'one_on_ones':               one_on_ones,
        # 2. Location
        'shots_in_box':              in_box_n,
        'shots_out_of_box':          out_box_n,
        'shots_six_yard_box':        six_yard,
        'in_box_conversion_pct':     in_box_conv,
        'avg_shot_distance_m':       avg_dist,
        # 3. Body part & technique
        'shots_right_foot':          right_foot,
        'shots_left_foot':           left_foot,
        'shots_header':              headers,
        'shots_volley':              volleys,
        'shots_first_touch':         first_touch,
        # 4. Pattern / situation
        'shots_from_corner':         from_corner,
        'shots_from_freekick':       from_fk,
        'shots_fast_break':          fast_break,
        'shots_assisted':            assisted,
        'shots_individual_play':     individual,
        # 5. Open play splits
        'shots_open_play':           op_shots,
        'goals_open_play':           op_goals,
        'shots_on_target_open_play': op_sot,
        # 6. Goal mouth placement (on-target shots)
        'placement_top_corner':      top_corner,
        'placement_low_corner':      low_corner,
        'placement_centre_frame':    centre_frame,
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def _attach_sca_totals(stat, rows):
    """Team-level SCA totals: count creating actions across the team's shots."""
    kinds = defaultdict(int)
    n_sca1 = n_sca2 = 0
    for r in rows:
        if r.get('sca_1_type'):
            kinds[r['sca_1_type']] += 1; n_sca1 += 1
        if r.get('sca_2_type'):
            kinds[r['sca_2_type']] += 1; n_sca2 += 1
    stat['sca_total']     = n_sca1 + n_sca2
    stat['sca_pass_live'] = kinds.get('Pass (Live)', 0)
    stat['sca_pass_dead'] = kinds.get('Pass (Dead)', 0)
    stat['sca_take_on']   = kinds.get('Take-On', 0)
    stat['sca_shot']      = kinds.get('Shot', 0)
    stat['sca_fouled']    = kinds.get('Fouled', 0)
    stat['sca_defense']   = kinds.get('Interception', 0) + kinds.get('Tackle', 0)
    # shots with at least one creating action (the rest are unassisted/direct)
    stat['shots_with_sca'] = sum(1 for r in rows if r.get('sca_1_type'))


def parse_shooting(data: dict, whoscored_match_id: int) -> dict:
    """
    Parse all shooting stats from WhoScored matchCentreData.

    Returns:
        {
            'team':  [home_row, away_row]     — team-level stats
            'player': [...]                   — player-level stats
            'shots':  [...]                   — per-shot rows for xG viz / joining Fotmob xGOT
        }
    """
    events  = data.get('events', [])
    home_id = data['home']['teamId']
    away_id = data['away']['teamId']
    names   = data.get('playerIdNameDictionary', {})

    # ── All shot events ────────────────────────────────────────────────────────
    shot_events = [
        e for e in events
        if e.get('isShot')
        and e.get('type', {}).get('value') in SHOT_TYPES
    ]

    # Build per-shot rows
    shot_rows = [_shot_row(e, whoscored_match_id, names) for e in shot_events]

    # Chronological row_id (the WhoScored feed ships events in chronological order,
    # see the SCA note below). 0-indexed to match the Sofascore scheme, so
    # (combo_id, row_id) is the shared shot key across all three providers.
    for _i, _r in enumerate(shot_rows):
        _r['row_id'] = _i

    # ── Shot-creating actions (SCA) ─────────────────────────────────────────────
    # Order: use WhoScored's delivered event order (the feed ships events
    # already in chronological/sequence order). We deliberately do NOT re-sort
    # by `eventId` (not unique within a match) or by `id` (authentic in clean
    # feeds but scrambled in some archived/re-exported files, so unreliable as
    # a universal key). Locate shots by object identity, never by id.
    sorted_events = list(events)
    # eventId is NOT unique in WhoScored data, so map by object identity.
    pos_by_id = {id(e): i for i, e in enumerate(sorted_events)}
    # Per-player SCA credit (a player earns SCA on OTHER players' shots too).
    sca_credit = defaultdict(lambda: defaultdict(int))  # (team,pid) -> kind -> n
    for e, row in zip(shot_events, shot_rows):
        pos = pos_by_id.get(id(e))
        sca = _resolve_sca(sorted_events, pos) if pos is not None else []
        for slot, (sev, kind) in zip(('sca_1', 'sca_2'), sca):
            spid = sev.get('playerId')
            row[f'{slot}_player_id'] = spid
            row[f'{slot}_player_name'] = names.get(str(spid))
            row[f'{slot}_type'] = kind
            if spid:
                sca_credit[(e.get('teamId'), spid)][kind] += 1
        for slot in ('sca_1', 'sca_2')[len(sca):]:
            row[f'{slot}_player_id'] = None
            row[f'{slot}_player_name'] = None
            row[f'{slot}_type'] = None

    # ── Team-level ─────────────────────────────────────────────────────────────
    team_stats = []
    for team_id in (home_id, away_id):
        rows = [r for r in shot_rows if r['team_id'] == team_id]
        stat = _aggregate(rows, whoscored_match_id, team_id, None, None)
        if stat:
            _attach_sca_totals(stat, rows)
            team_stats.append(stat)

    # ── Player-level ───────────────────────────────────────────────────────────
    player_map = defaultdict(list)
    for r in shot_rows:
        if r['player_id']:
            player_map[(r['team_id'], r['player_id'])].append(r)
    # ensure players who only have SCA credit (no shots) still get a row
    for key in sca_credit:
        player_map.setdefault(key, [])

    player_stats = []
    for (team_id, player_id), rows in player_map.items():
        stat = _aggregate(rows, whoscored_match_id, team_id, player_id,
                          names.get(str(player_id)))
        if stat is None:
            # SCA-only player (no shots): build a minimal row
            stat = {
                'whoscored_match_id': whoscored_match_id,
                'team_id': team_id, 'player_id': player_id,
                'player_name': names.get(str(player_id)),
            }
        credit = sca_credit.get((team_id, player_id), {})
        stat['sca'] = sum(credit.values())
        stat['sca_pass_live'] = credit.get('Pass (Live)', 0)
        stat['sca_pass_dead'] = credit.get('Pass (Dead)', 0)
        stat['sca_take_on']   = credit.get('Take-On', 0)
        stat['sca_shot']      = credit.get('Shot', 0)
        stat['sca_fouled']    = credit.get('Fouled', 0)
        stat['sca_defense']   = credit.get('Interception', 0) + credit.get('Tackle', 0)
        player_stats.append(stat)

    return {
        'team':   team_stats,
        'player': player_stats,
        'shots':  shot_rows,
    }



if __name__ == "__main__":
    import json
    path = './1903468.json'
    with open(path, "r", encoding='utf-8') as file:
        file = json.load(file)

    result = parse_shooting(file, 1729476)
    with open("./result.json", "w", encoding='utf-8') as fp:
        json.dump(result , fp, ensure_ascii=False)