"""
parse_possession_advanced.py
----------------------------
Derives advanced possession metrics from WhoScored matchCentreData event sequences.

POSSESSION SEQUENCES
--------------------
A possession sequence = consecutive events by the same team until:
  - The opponent makes a SUCCESSFUL on-ball action (teamId changes + outcome=Successful)
  - A dead-ball event occurs (corner awarded, offside, substitution, etc.)

Unsuccessful defensive actions by the opposing team (failed tackles, lost aerial
duels, failed challenges) do NOT break a sequence — the possessing team retains
the ball. This matches Opta's sequence definition.

Each sequence is characterised by:
  - Length (event count)
  - Duration (seconds from first to last event)
  - Start x / end x (territory covered)
  - Outcome (shot, turnover, set piece, etc.)

SHOT OUTCOMES
--------------
  - Goal           : shot resulting in a goal
  - SavedShot      : shot on target saved by goalkeeper (SavedShot WITHOUT 'Blocked' qualifier)
  - ShotBlocked    : shot blocked by a defender (SavedShot WITH 'Blocked' qualifier)
  - MissedShots    : shot off target

PASSING RATE
--------------
Accurate passes per possession minute (possession% × 90).

ATTACK SPEED
--------------
For sequences that reach the final third (x > 66.6):
  - Duration (seconds from sequence start to first final-third event)
  - Passes played before reaching final third

COUNTER-ATTACK SEQUENCES
--------------------------
Sequences starting in own half (start_x ≤ 50) that reach the final third
(any event with x > 66.6) within 10 seconds. Fast transition play.

FIELD TILT
-----------
Final third touch share: team_final_third_touches / (home + away final_third_touches).
"""

import math
from collections import defaultdict


# ── Constants ─────────────────────────────────────────────────────────────────

FINAL_THIRD_MIN   = 66.6
COUNTER_MAX_SECS  = 10.0   # sequences reaching final third in ≤10s = counter-attack
LONG_SEQ_MIN      = 10     # sequences with ≥10 events = sustained possession
MIN_SEQ_EVENTS    = 2      # minimum events to count as a possession sequence

# Outcomes that represent a shot ending a sequence
_SHOT_OUTCOMES = {'goal', 'saved', 'blocked', 'off_target'}

# Event types that end a possession sequence (true dead ball restarts)
_DEAD_BALL = {
    'End', 'Start', 'FormationSet', 'FormationChange',
    'SubstitutionOn', 'SubstitutionOff', 'OffsideGiven',
}

# Events that are skipped entirely during sequence building —
# they are not dead balls (they don't break a sequence) but they
# are also not play events. CornerAwarded is administrative.
# Card events happen post-action and should not extend the sequence.
_SKIP_EVENTS = {'CornerAwarded', 'Card'}

# Contested event types — when the OPPOSING team produces one of these
# with outcome=Unsuccessful, it means they FAILED to win the ball.
# The possessing team still has it → do NOT break the sequence.
_CONTESTED = {'Tackle', 'Challenge', 'Aerial'}

# Single-event "micro-possessions" that don't constitute real possession.
# When team A → single micro-event by team B → team A, we merge them into
# one continuous sequence for team A. These are reflexive defensive touches
# (clearances, deflections, blocked passes/shots) not deliberate possession.
_MICRO_POSSESSION = {'Clearance', 'BallTouch', 'BlockedPass', 'Punch'}

# Pass qualifier IDs excluded from base pass count (same as parse_passing baseline)
# QID 2=Cross/CornerTaken, QID 107=ThrowIn, QID 123=KeeperThrow
_PASS_EXCL = {2, 107, 123}

# Event types that represent a defensive action winning the ball
_DEFENSIVE_WON = {
    'Tackle', 'Challenge', 'Interception', 'BlockedPass',
    'BallRecovery', 'KeeperPickup', 'KeeperSweeper', 'Save',
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_time(event: dict) -> float:
    return event.get('expandedMinute', 0) * 60 + (event.get('second') or 0)

def _flt(v, default=0.0) -> float:
    try: return float(v)
    except: return default

def _has_qualifier(event: dict, qual_name: str) -> bool:
    """Check if an event has a specific qualifier by displayName."""
    return any(
        q.get('type', {}).get('displayName') == qual_name
        for q in event.get('qualifiers', [])
    )


# ── Sequence builder ──────────────────────────────────────────────────────────

def _build_sequences(sorted_events: list) -> list:
    """
    Split event stream into possession sequences.
    Returns list of (team_id, [events], dead_ball_type, next_event) tuples.

    Key rule: unsuccessful contested actions (failed tackles, lost aerial duels,
    failed challenges) by the OPPOSING team do NOT break the sequence. The
    possessing team still has the ball.

    dead_ball_type : str | None  — the _DEAD_BALL event type that ended the sequence
    next_event     : dict | None — the first event of the next team's possession
    """
    sequences    = []
    current_team = None
    current_seq  = []

    for e in sorted_events:
        etype   = e['type']['displayName']
        team    = e.get('teamId')
        outcome = e.get('outcomeType', {}).get('displayName')

        # ── Dead ball → flush current sequence ──
        if etype in _DEAD_BALL:
            if current_seq:
                sequences.append((current_team, current_seq, etype, None))
            current_seq  = []
            current_team = None
            continue

        # ── Skip administrative events ──
        if etype in _SKIP_EVENTS:
            continue

        if not team:
            continue

        if team != current_team:
            # Opposing team event — check if it's a FAILED contested action
            if (current_seq
                    and etype in _CONTESTED
                    and outcome == 'Unsuccessful'):
                # Defender failed to win the ball → possessing team keeps it.
                # Skip this event entirely — don't add it to any sequence.
                continue

            # Genuine possession change
            if current_seq:
                sequences.append((current_team, current_seq, None, e))
            current_seq  = [e]
            current_team = team
        else:
            current_seq.append(e)

        # ── Shot → flush immediately (shots are terminal events) ──
        # Fires whether the shot started a new sequence or continued one.
        if e.get('isShot') and current_seq:
            sequences.append((current_team, current_seq, None, None))
            current_seq  = []
            current_team = None

    if current_seq:
        sequences.append((current_team, current_seq, 'End', None))

    # ── Merge through micro-possessions ───────────────────────────────
    # When team A → single-event micro-possession by team B → team A,
    # merge into one continuous team A sequence. Clearances, deflections,
    # blocked passes are reflexive defensive actions, not real possession.
    merged = []
    i = 0
    while i < len(sequences):
        tid, seq, db, ne = sequences[i]

        # Look ahead: is the next sequence a single-event micro-possession
        # by the opposing team, followed by a sequence by our same team?
        while (i + 2 < len(sequences)
               and db is None                          # current didn't end on dead ball
               and not seq[-1].get('isShot')           # shots are terminal — never merge past them
               and sequences[i+1][0] != tid            # next is opponent
               and len(sequences[i+1][1]) == 1         # next is single event
               and sequences[i+1][1][0]['type']['displayName'] in _MICRO_POSSESSION
               and sequences[i+1][2] is None           # micro didn't end on dead ball
               and sequences[i+2][0] == tid):          # sequence after is same team
            # Merge: absorb the micro-possession event + the next same-team sequence
            micro_seq = sequences[i+1][1]
            next_tid, next_seq, next_db, next_ne = sequences[i+2]
            seq = seq + micro_seq + next_seq
            db  = next_db
            ne  = next_ne
            i  += 2  # skip the micro + the merged sequence

        merged.append((tid, seq, db, ne))
        i += 1

    return merged


def _seq_outcome(seq: list, dead_ball: str | None, next_event: dict | None) -> str | None:
    """
    Classify how a sequence ended (from the possessing team's perspective):

    Shot endings:
      - goal         : shot resulting in a goal
      - saved        : shot on target, saved by GK
      - blocked      : shot blocked by a defender
      - off_target   : shot missed the target

    Non-shot endings:
      - turnover     : lost the ball without a clean defensive action
      - dispossessed : opponent won ball via tackle / interception / recovery
      - foul_won     : team was fouled — free kick to them
      - foul_conceded: team committed a foul — free kick against them
      - offside      : sequence ended by offside call

    Returns None for admin stops (substitution, end of half) → excluded from output.
    """
    last  = seq[-1]
    ltype = last['type']['displayName']

    # 1. Shot — check isShot flag, then distinguish GK save vs defender block
    if last.get('isShot'):
        if ltype == 'SavedShot':
            return 'blocked' if _has_qualifier(last, 'Blocked') else 'saved'
        if ltype == 'Goal':
            return 'goal'
        return 'off_target'   # MissedShots or any other shot type

    # 2. Foul on the last event
    if ltype == 'Foul':
        acc = last.get('outcomeType', {}).get('displayName') == 'Successful'
        return 'foul_won' if acc else 'foul_conceded'

    # 3. Dead ball endings
    if dead_ball == 'OffsideGiven':
        return 'offside'
    if dead_ball in ('End', 'SubstitutionOff', 'SubstitutionOn',
                     'FormationChange', 'FormationSet'):
        return None   # exclude admin stops

    # 4. Opponent's first event determines how they won it back
    if next_event:
        if next_event['type']['displayName'] in _DEFENSIVE_WON:
            return 'dispossessed'
        return 'turnover'

    return None


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_possession_advanced(data: dict, whoscored_match_id: int) -> dict:
    """
    Compute advanced possession metrics from WhoScored matchCentreData.

    Returns:
        {
            'team':      [home_row, away_row]
            'sequences': [one row per possession sequence]
        }
    """
    events  = data.get('events', [])
    home_id = data['home']['teamId']
    away_id = data['away']['teamId']

    sorted_events = sorted(
        events,
        key=lambda e: (_get_time(e), e.get('eventId', 0))
    )

    # ── Possession % (for passing rate denominator) ────────────────────────────
    home_poss_sum = sum(_flt(v) for v in data['home']['stats'].get('possession', {}).values())
    away_poss_sum = sum(_flt(v) for v in data['away']['stats'].get('possession', {}).values())
    total_poss    = home_poss_sum + away_poss_sum
    poss_pct = {
        home_id: home_poss_sum / total_poss if total_poss else 0.5,
        away_id: away_poss_sum / total_poss if total_poss else 0.5,
    }
    poss_minutes = {tid: pct * 90 for tid, pct in poss_pct.items()}

    # ── Touch counts for field tilt ────────────────────────────────────────────
    ft_touches = {home_id: 0, away_id: 0}
    for e in events:
        if e.get('isTouch') and e.get('teamId') in (home_id, away_id):
            if _flt(e.get('x', 0)) > FINAL_THIRD_MIN:
                ft_touches[e['teamId']] += 1

    total_ft = ft_touches[home_id] + ft_touches[away_id]
    field_tilt = {
        home_id: round(ft_touches[home_id] / total_ft * 100, 1) if total_ft else 50.0,
        away_id: round(ft_touches[away_id] / total_ft * 100, 1) if total_ft else 50.0,
    }

    # ── Passing rate (base passes, same exclusions as parse_passing) ───────────
    acc_passes = {home_id: 0, away_id: 0}
    for e in events:
        if (e['type']['displayName'] == 'Pass'
                and e.get('teamId') in (home_id, away_id)
                and e.get('outcomeType', {}).get('displayName') == 'Successful'
                and not any(any(q.get('type', {}).get('value') == qid
                                for q in e.get('qualifiers', []))
                            for qid in _PASS_EXCL)):
            acc_passes[e['teamId']] += 1

    passing_rate = {
        tid: round(acc_passes[tid] / poss_minutes[tid], 2) if poss_minutes[tid] else None
        for tid in (home_id, away_id)
    }

    # ── Sequence-level analysis ────────────────────────────────────────────────
    sequences = _build_sequences(sorted_events)

    team_agg = {tid: {
        'seq_count':                 0,
        'seq_length_sum':            0,
        'seq_duration_sum':          0.0,
        'seq_long_count':            0,
        'seq_shot_count':            0,
        'seq_shot_length_sum':       0,
        'seq_shot_duration_sum':     0.0,
        'seq_shot_passes_sum':       0,
        'seq_attack_count':          0,
        'seq_attack_duration_sum':   0.0,
        'seq_attack_passes_sum':     0,
        'seq_counter_count':         0,
        'seq_counter_shot_count':    0,
        'seq_start_x_sum':           0.0,
        'seq_turnover_count':        0,
        'seq_setpiece_count':        0,
    } for tid in (home_id, away_id)}

    sequence_rows = []

    for team_id, seq, dead_ball, next_event in sequences:
        if team_id not in (home_id, away_id): continue
        a = team_agg[team_id]

        length   = len(seq)
        t_start  = _get_time(seq[0])
        t_end    = _get_time(seq[-1])
        duration = t_end - t_start

        x_vals   = [_flt(e.get('x', 50)) for e in seq if e.get('x') is not None]
        start_x  = x_vals[0] if x_vals else 50.0
        end_x    = x_vals[-1] if x_vals else 50.0

        outcome  = _seq_outcome(seq, dead_ball, next_event)

        # Exclude sequences that can't be classified
        if outcome is None:
            continue

        is_shot  = outcome in _SHOT_OUTCOMES

        # Filter out single-touch sequences — unless they end in a shot
        if length < MIN_SEQ_EVENTS and not is_shot:
            continue

        is_long  = length >= LONG_SEQ_MIN

        n_passes = sum(1 for e in seq if e['type']['displayName'] == 'Pass')

        # Players involved (ordered by first appearance, unique)
        player_ids_seq = list(dict.fromkeys(
            e.get('playerId') for e in seq if e.get('playerId')
        ))
        player_id_start  = seq[0].get('playerId')
        player_id_end    = seq[-1].get('playerId')
        event_id_start   = seq[0].get('eventId')
        event_id_end     = seq[-1].get('eventId')

        # Attacking sequence = reached final third
        reaches_ft = any(_flt(e.get('x', 0)) > FINAL_THIRD_MIN for e in seq)
        is_attack  = reaches_ft and length >= 2

        # Counter-attack = starts in own half, reaches final third quickly
        is_counter = (start_x <= 50.0 and reaches_ft)
        if is_counter:
            ft_time = None
            for e in seq:
                if _flt(e.get('x', 0)) > FINAL_THIRD_MIN:
                    ft_time = _get_time(e) - t_start
                    break
            is_counter = (ft_time is not None and ft_time <= COUNTER_MAX_SECS)

        # ── Accumulate ────────────────────────────────────────────────────
        a['seq_count']          += 1
        a['seq_length_sum']     += length
        a['seq_duration_sum']   += duration
        a['seq_start_x_sum']    += start_x
        if is_long:  a['seq_long_count']  += 1
        if is_shot:
            a['seq_shot_count']          += 1
            a['seq_shot_length_sum']     += length
            a['seq_shot_duration_sum']   += duration
            a['seq_shot_passes_sum']     += n_passes
        if is_attack:
            a['seq_attack_count']          += 1
            a['seq_attack_duration_sum']   += duration
            a['seq_attack_passes_sum']     += n_passes
        if is_counter:
            a['seq_counter_count']     += 1
            if is_shot: a['seq_counter_shot_count'] += 1

        # Turnover = lost possession without a shot or dead ball
        if outcome in ('turnover', 'dispossessed'):
            a['seq_turnover_count'] += 1
        # Set piece end = sequence ended in a dead-ball restart situation
        if outcome in ('foul_won', 'foul_conceded', 'offside'):
            a['seq_setpiece_count'] += 1

        # Per-sequence row
        sequence_rows.append({
            'whoscored_match_id': whoscored_match_id,
            'team_id':            team_id,
            'length':             length,
            'duration_s':         round(duration, 1),
            'start_x':            round(start_x, 1),
            'start_y':            round(_flt(seq[0].get('y', 50)), 1),
            'end_x':              round(end_x, 1),
            'end_y':              round(_flt(seq[-1].get('y', 50)), 1),
            'n_passes':           n_passes,
            'is_shot':            is_shot,
            'is_long':            is_long,
            'is_attack':          is_attack,
            'is_counter':         is_counter,
            'outcome':            outcome,
            'event_id_start':     event_id_start,
            'event_id_end':       event_id_end,
            'player_id_start':    player_id_start,
            'player_id_end':      player_id_end,
            'player_ids':         player_ids_seq,
        })

    # ── Build team rows ────────────────────────────────────────────────────────
    team_rows = []
    for tid in (home_id, away_id):
        a   = team_agg[tid]
        sc  = a['seq_count']
        shc = a['seq_shot_count']
        atk = a['seq_attack_count']

        avg_seq_len  = round(a['seq_length_sum']   / sc, 1)  if sc  else None
        avg_seq_dur  = round(a['seq_duration_sum']  / sc, 1)  if sc  else None
        avg_start_x  = round(a['seq_start_x_sum']   / sc, 1)  if sc  else None
        avg_shot_len = round(a['seq_shot_length_sum'] / shc, 1) if shc else None
        avg_shot_dur = round(a['seq_shot_duration_sum'] / shc, 1) if shc else None
        avg_shot_pass= round(a['seq_shot_passes_sum'] / shc, 1) if shc else None
        avg_atk_dur  = round(a['seq_attack_duration_sum'] / atk, 1) if atk else None
        avg_atk_pass = round(a['seq_attack_passes_sum']   / atk, 1) if atk else None
        turn_pct     = round(a['seq_turnover_count'] / sc * 100, 1) if sc else None
        sp_pct       = round(a['seq_setpiece_count'] / sc * 100, 1) if sc else None
        shot_eff     = round(shc / sc * 100, 1) if sc else None
        pm           = poss_minutes[tid]

        team_rows.append({
            'whoscored_match_id':            whoscored_match_id,
            'team_id':                       tid,
            # Passing rate
            'passing_rate':                  passing_rate[tid],
            'passes_accurate':               acc_passes[tid],
            'possession_minutes':            round(pm, 1),
            # Field tilt
            'field_tilt_pct':                field_tilt[tid],
            'final_third_touches':           ft_touches[tid],
            # Sequences
            'sequences_total':               sc,
            'avg_sequence_length':           avg_seq_len,
            'avg_sequence_duration_s':       avg_seq_dur,
            'avg_sequence_start_x':          avg_start_x,
            'long_sequences':                a['seq_long_count'],
            'long_seq_pct':                  round(a['seq_long_count'] / sc * 100, 1) if sc else None,
            # Shot-ending sequences
            'shot_sequences':                shc,
            'shot_seq_pct':                  shot_eff,
            'avg_shot_seq_length':           avg_shot_len,
            'avg_shot_seq_duration_s':       avg_shot_dur,
            'avg_shot_seq_passes':           avg_shot_pass,
            # Attack speed (sequences reaching final third)
            'attack_sequences':              atk,
            'avg_attack_duration_s':         avg_atk_dur,
            'avg_attack_passes':             avg_atk_pass,
            # Counter-attacks
            'counter_attack_sequences':      a['seq_counter_count'],
            'counter_attack_shot_pct':       round(a['seq_counter_shot_count'] / a['seq_counter_count'] * 100, 1)
                                             if a['seq_counter_count'] else None,
            # Sequence endings
            'turnover_pct':                  turn_pct,
            'set_piece_end_pct':             sp_pct,
        })

    return {
        'team':      team_rows,
        'sequences': sequence_rows,
    }



if __name__ == "__main__":
    import json
    path = './1903468.json'
    # path = './whoscored_data/new_sun.json'
    with open(path, "r", encoding='utf-8') as file:
        file = json.load(file)

    result = parse_possession_advanced(file, 1729476)
    with open("./result_advanced.json", "w", encoding='utf-8') as fp:
        json.dump(result , fp, indent=4)