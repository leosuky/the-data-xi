"""
parse_possession_advanced.py  (v3 — Opta-aligned two-level framework)
---------------------------------------------------------------------
Derives advanced possession metrics from WhoScored matchCentreData events.

THE OPTA FRAMEWORK (two levels)
-------------------------------
SEQUENCE   = a passage of play belonging to one team, ended by a defensive
             action, a stoppage in play, or a shot.        (Opta definition)
POSSESSION = one or more consecutive sequences belonging to the same team.
             Ends only when the OPPOSITION GAINS CONTROL.  (Opta definition)

Example: passes -> shot saved -> corner won -> cross -> header wide is
ONE possession but THREE sequences (shot ends seq 1, the corner restart
starts seq 2, the second shot ends seq 3).

Earlier versions of this parser produced a single hybrid level (sequences
merged across lone defensive touches). v3 produces both levels so that
sequence stats are directly comparable to published Opta numbers.

SEQUENCE RULES
--------------
  - Unsuccessful contested actions (failed tackles, lost aerials, failed
    challenges) by the opposing team do NOT end a sequence.
  - Fouls end the sequence deterministically: the foul pair (fouled player
    outcome=Successful / fouler outcome=Unsuccessful, arbitrary order) is
    resolved relative to the possessing team. Fouls never extend sequences.
  - Any event at a set-piece restart (throw-in / free kick / corner / goal
    kick / kick-off qualifier) starts a NEW sequence: the ball was dead, so
    durations never span out-of-play time.
  - Lone reflexive defensive touches (clearance, deflection, blocked pass,
    punch) do not form sequences of their own, do not end the possession,
    and — when the same attacking team immediately regains the ball — do not
    even split that team's SEQUENCE. The bracketing fragments are stitched
    back into one sequence (the touch never established control). This is the
    same possession principle validated in parse_shooting's SCA work. A
    genuine turnover (Interception, completed opponent pass, opponent shot)
    still ends the sequence.
  - Own goals end the attacking team's sequence with outcome 'goal'.

POSSESSION CHAINING
-------------------
Consecutive sequences by the same team chain into one possession. Lone
opponent micro-touches are transparent. Restarts retained by the same team
(corner won, throw-in, free kick won) continue the possession.

OPTA-STYLE TEAM METRICS (definitions verified against Stats Perform /
The Analyst, May 2025)
----------------------------------------------------------------------
  - avg_sequence_time / passes_per_sequence
  - ten_plus_pass_sequences : open-play sequences with 10+ passes
  - build_up_attacks  : open-play sequences with 10+ passes ending in a
                        shot or >=1 touch in the opposition box
  - direct_attacks    : open-play sequences starting inside own half with
                        >=50% of movement towards the opposition goal,
                        ending in a shot or a box touch
  - direct_speed_m_s  : upfield progress (m) divided by sequence time,
                        over open-play sequences starting in own half
  - start_distance_m  : avg distance (m) from own goal of open-play
                        sequence starts
  - high_turnovers    : possessions starting in open play within 40m of
                        the opponent's goal (+ how many end in a shot)
  - ppda              : opposition passes outside the pressing team's own
                        defensive third / pressing team's defensive actions
                        (fouls, tackles, interceptions, challenges, blocked
                        passes) outside their own defensive third

TURNOVER METRICS
----------------
  - possession_loss_pct    : share of sequences ending in ANY loss of the
                             ball (open-play turnover, dispossession, or
                             out of play). Broad measure (renamed from the
                             old 'turnover_pct').
  - open_play_turnover_pct : STRICT measure — share of sequences ending
                             with the opponent winning the ball in open
                             play only (turnover + dispossessed). Excludes
                             out-of-play, fouls, offsides, shots.

FIELD TILT
----------
  - field_tilt_pct        : final-third TOUCH share (kept for continuity)
  - field_tilt_passes_pct : final-third completed-PASS share (the variant
                            usually quoted alongside Opta data)

UNITS: pitch scaled to 105m x 68m from WhoScored 0-100 coordinates.
"""

import math
from collections import defaultdict


# ── Constants ─────────────────────────────────────────────────────────────────

PITCH_LENGTH      = 105.0
PITCH_WIDTH       = 68.0
FINAL_THIRD_MIN   = 66.6
DEF_THIRD_MAX     = 33.4   # own defensive third boundary (own frame)
COUNTER_MAX_SECS  = 10.0   # legacy counter metric (kept for continuity)
LONG_SEQ_MIN      = 10
HIGH_TURNOVER_M   = 40.0   # high turnover = regain within 40m RADIUS of opp goal
                           # (Opta draw this as an arc around the goal, not a line)
BOX_X_MIN         = 100 - 16.5 / PITCH_LENGTH * 100   # 84.3
BOX_Y_MIN         = 50 - (40.32 / PITCH_WIDTH / 2) * 100  # 20.4
BOX_Y_MAX         = 50 + (40.32 / PITCH_WIDTH / 2) * 100  # 79.6

_SHOT_OUTCOMES = {'goal', 'saved', 'blocked', 'off_target'}

_DEAD_BALL = {
    'End', 'Start', 'FormationSet', 'FormationChange',
    'SubstitutionOn', 'SubstitutionOff', 'OffsideGiven',
}

_SKIP_EVENTS = {'CornerAwarded', 'Card'}

_CONTESTED = {'Tackle', 'Challenge', 'Aerial'}

# Reflexive defensive touches — never form sequences on their own and are
# transparent for possession chaining.
_MICRO_POSSESSION = {'Clearance', 'BallTouch', 'BlockedPass', 'Punch'}

# Set-piece restart qualifiers — the ball was DEAD before this event.
_RESTART_Q = {'ThrowIn', 'FreekickTaken', 'CornerTaken', 'GoalKick', 'KickOff'}

# Pass qualifier IDs excluded from base pass count (same as parse_passing baseline)
# QID 2=Cross, QID 107=ThrowIn, QID 123=KeeperThrow.
# NB: corners are QID 6 (CornerTaken) and are NOT excluded here unless they
# also carry the Cross qualifier — kept as-is to stay consistent with
# parse_passing; align both files if corners should be excluded.
_PASS_EXCL = {2, 107, 123}

_DEFENSIVE_WON = {
    'Tackle', 'Challenge', 'Interception', 'BlockedPass',
    'BallRecovery', 'KeeperPickup', 'KeeperSweeper', 'Save',
}

# PPDA defensive actions (Opta definition)
_PPDA_ACTIONS = {'Foul', 'Tackle', 'Interception', 'Challenge', 'BlockedPass'}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_time(event: dict) -> float:
    return event.get('expandedMinute', 0) * 60 + (event.get('second') or 0)

def _flt(v, default=0.0) -> float:
    try: return float(v)
    except: return default

def _has_qualifier(event: dict, qual_name: str) -> bool:
    return any(q.get('type', {}).get('displayName') == qual_name
               for q in event.get('qualifiers', []))

def _is_restart(event: dict) -> bool:
    """Event takes place at a set-piece restart (ball was dead before it)."""
    return any(q.get('type', {}).get('displayName') in _RESTART_Q
               for q in event.get('qualifiers', []))

def _goal_dist_m(x, y) -> float:
    """Distance (m) to the centre of the opposition goal (x=100, y=50)."""
    return math.sqrt(((100 - x) * PITCH_LENGTH / 100) ** 2
                     + ((50 - y) * PITCH_WIDTH / 100) ** 2)

def _in_box(e) -> bool:
    x, y = _flt(e.get('x'), -1), _flt(e.get('y'), -1)
    return x >= BOX_X_MIN and BOX_Y_MIN <= y <= BOX_Y_MAX


# ── Sequence builder (RAW — Opta sequence level) ─────────────────────────────

def _build_sequences(sorted_events: list, home_id: int, away_id: int) -> list:
    """
    Split event stream into Opta-style sequences.
    Returns list of (team_id, [events], dead_ball_type, next_event) tuples.

    dead_ball_type : str | None  — why the sequence ended (dead-ball / foul /
                                   restart marker), None = open-play change
    next_event     : dict | None — the first event of the next passage
    """
    sequences    = []
    current_team = None
    current_seq  = []

    def flush(db, ne):
        nonlocal current_seq, current_team
        if current_seq:
            sequences.append((current_team, current_seq, db, ne))
        current_seq  = []
        current_team = None

    for e in sorted_events:
        etype   = e['type']['displayName']
        team    = e.get('teamId')
        outcome = e.get('outcomeType', {}).get('displayName')

        if etype in _DEAD_BALL:
            flush(etype, None)
            continue

        if etype in _SKIP_EVENTS:
            continue

        if not team:
            continue

        # ── Fouls: resolve the pair deterministically ──
        if etype == 'Foul':
            if current_seq:
                fouled = team if outcome == 'Successful' \
                         else (home_id if team == away_id else away_id)
                flush('FoulWon' if fouled == current_team else 'FoulConceded',
                      None)
            continue

        # ── Own goal: credit the attacking team's sequence with a goal ──
        if etype == 'Goal' and _has_qualifier(e, 'OwnGoal'):
            if current_seq and current_team != team:
                flush('OwnGoalFor', None)
            else:
                flush('End', None)
            continue

        # ── Set-piece restart: ball was dead → new sequence starts here ──
        if _is_restart(e):
            flush('Restart', e)
            current_seq  = [e]
            current_team = team
            continue

        if team != current_team:
            # Failed contested action by the opponent → seq continues
            if (current_seq
                    and etype in _CONTESTED
                    and outcome == 'Unsuccessful'):
                continue

            flush(None, e)
            current_seq  = [e]
            current_team = team
        else:
            current_seq.append(e)

        # ── Shot → flush immediately (shots end sequences — Opta) ──
        if e.get('isShot') and current_seq:
            flush(None, None)

    flush('End', None)
    return _stitch_reflexive_touches(sequences)


# Reactive opponent touches that do NOT establish control. Consistent with the
# possession logic validated in parse_shooting: a clearance / blocked pass /
# loose touch that the attacking team immediately regains is transparent — it
# does not start a new possession and should not split the attacking team's
# sequence. (An Interception, a successful Tackle that leads to opponent
# possession, or a completed opponent pass IS a genuine turnover and does
# split — those are never single-touch-bracketed by the same team anyway.)
_REFLEXIVE_TOUCH = {'Clearance', 'BlockedPass', 'BallTouch', 'Punch'}


def _stitch_reflexive_touches(sequences: list) -> list:
    """
    Merge sequence fragments that were split only by a single reflexive
    opponent touch (clearance / block / loose touch) which the SAME attacking
    team immediately regained. Such a touch did not transfer control, so the
    bracketing fragments are really one continuous sequence.

    Before: [team A seq] [team B lone Clearance] [team A seq]   (3 fragments)
    After:  [team A seq + team A seq]                            (1 sequence)

    The reflexive touch is DROPPED (its coords are in the opponent frame and
    its player is an opponent — keeping it would corrupt x-scans / pass counts,
    the same trap handled in the carries work).
    """
    out = []
    i = 0
    n = len(sequences)
    while i < n:
        tid, seq, db, ne = sequences[i]
        # look for: [tid seq, open-play] [opp lone reflexive touch, open-play] [tid ...]
        while (i + 2 < n
               and db is None
               and not seq[-1].get('isShot')
               and len(sequences[i + 1][1]) == 1
               and sequences[i + 1][0] is not None
               and sequences[i + 1][0] != tid
               and sequences[i + 1][1][0]['type']['displayName'] in _REFLEXIVE_TOUCH
               and sequences[i + 1][2] is None
               and sequences[i + 2][0] == tid):
            _, next_seq, next_db, next_ne = sequences[i + 2]
            seq = seq + next_seq          # reflexive touch dropped, not merged
            db, ne = next_db, next_ne
            i += 2
        out.append((tid, seq, db, ne))
        i += 1
    return out


def _is_real_sequence(seq: list) -> bool:
    """
    Opta: "not every event belongs to a sequence — a sequence starts with a
    player making a controlled action on the ball." A lone reflexive
    defensive touch (clearance / deflection / blocked pass / punch / lost
    aerial) is not a sequence.
    """
    if len(seq) >= 2:
        return True
    return seq[0]['type']['displayName'] not in (_MICRO_POSSESSION | {'Aerial', 'Save'})


def _chain_possessions(sequences: list) -> list:
    """
    Group consecutive sequences by the same team into possessions.
    Lone opponent micro-touches are transparent (the opposition did not
    gain control). Same-team restarts (corner won, throw-in) continue
    the possession.

    Returns a list of possessions: each is a list of indices into
    `sequences` (only "real" sequences are indexed).
    """
    possessions = []
    current = []
    current_team = None

    for idx, (tid, seq, db, ne) in enumerate(sequences):
        if not _is_real_sequence(seq):
            continue   # transparent: does not end the possession
        if tid == current_team:
            current.append(idx)
        else:
            if current:
                possessions.append((current_team, current))
            current = [idx]
            current_team = tid
    if current:
        possessions.append((current_team, current))
    return possessions


# ── Outcome classification ────────────────────────────────────────────────────

def _seq_outcome(seq: list, dead_ball: str | None, next_event: dict | None) -> str | None:
    """
    Classify how a sequence ended (possessing team's perspective):

    Shot endings:    goal / saved / blocked / off_target
    Non-shot:
      - turnover     : lost the ball in open play (no clean defensive action)
      - dispossessed : opponent won the ball via tackle/interception/recovery
      - foul_won / foul_conceded
      - offside
      - set_piece_won: ball out of play, same team restarts (throw/corner won)
      - out_of_play  : ball out of play, opponent restarts

    Returns None for admin stops (substitution, end of half).
    """
    last  = seq[-1]
    ltype = last['type']['displayName']

    if last.get('isShot'):
        if ltype == 'SavedShot':
            return 'blocked' if _has_qualifier(last, 'Blocked') else 'saved'
        if ltype == 'Goal':
            return 'goal'
        return 'off_target'

    if dead_ball == 'FoulWon':
        return 'foul_won'
    if dead_ball == 'FoulConceded':
        return 'foul_conceded'
    if dead_ball == 'OwnGoalFor':
        return 'goal'
    if dead_ball == 'Restart':
        if next_event is not None and next_event.get('teamId') == seq[-1].get('teamId'):
            return 'set_piece_won'
        return 'out_of_play'

    if dead_ball == 'OffsideGiven':
        return 'offside'
    if dead_ball in ('End', 'SubstitutionOff', 'SubstitutionOn',
                     'FormationChange', 'FormationSet'):
        return None

    if next_event:
        if next_event['type']['displayName'] in _DEFENSIVE_WON:
            return 'dispossessed'
        return 'turnover'

    return None


# ── Sequence geometry helpers ────────────────────────────────────────────────

def _seq_xs(seq):
    return [_flt(e.get('x')) for e in seq if e.get('x') is not None]

def _upfield_share(seq) -> float:
    """Share of total x-movement that goes towards the opposition goal."""
    xs = _seq_xs(seq)
    if len(xs) < 2:
        return 0.0
    fwd  = sum(max(0.0, xs[i+1] - xs[i]) for i in range(len(xs) - 1))
    tot  = sum(abs(xs[i+1] - xs[i])      for i in range(len(xs) - 1))
    return fwd / tot if tot > 0 else 0.0

def _ends_shot_or_box(seq) -> bool:
    return bool(seq[-1].get('isShot')) or any(
        e.get('isTouch') and _in_box(e) for e in seq)


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_possession_advanced(data: dict, whoscored_match_id: int) -> dict:
    """
    Compute advanced possession metrics from WhoScored matchCentreData.

    Returns:
        {
            'team':      [home_row, away_row],
            'sequences': [one row per Opta-style sequence, with possession_id]
        }
    """
    events  = data.get('events', [])
    home_id = data['home']['teamId']
    away_id = data['away']['teamId']

    sorted_events = sorted(events, key=lambda e: (_get_time(e), e.get('eventId', 0)))

    # ── Possession % / minutes ────────────────────────────────────────────────
    home_poss_sum = sum(_flt(v) for v in data['home']['stats'].get('possession', {}).values())
    away_poss_sum = sum(_flt(v) for v in data['away']['stats'].get('possession', {}).values())
    total_poss    = home_poss_sum + away_poss_sum
    poss_pct = {
        home_id: home_poss_sum / total_poss if total_poss else 0.5,
        away_id: away_poss_sum / total_poss if total_poss else 0.5,
    }
    # Actual playing length (incl. stoppage) — a flat 90 inflates passing_rate
    match_minutes = max((e.get('expandedMinute', 0) for e in events), default=90) or 90
    poss_minutes  = {tid: pct * match_minutes for tid, pct in poss_pct.items()}

    # ── Field tilt (touch share + completed-pass share) ───────────────────────
    ft_touches = {home_id: 0, away_id: 0}
    ft_passes  = {home_id: 0, away_id: 0}
    for e in events:
        tid = e.get('teamId')
        if tid not in (home_id, away_id):
            continue
        if _flt(e.get('x', 0)) > FINAL_THIRD_MIN:
            if e.get('isTouch'):
                ft_touches[tid] += 1
            if (e['type']['displayName'] == 'Pass'
                    and e.get('outcomeType', {}).get('displayName') == 'Successful'):
                ft_passes[tid] += 1

    def _share(d):
        tot = d[home_id] + d[away_id]
        return ({home_id: round(d[home_id] / tot * 100, 1),
                 away_id: round(d[away_id] / tot * 100, 1)}
                if tot else {home_id: 50.0, away_id: 50.0})
    field_tilt        = _share(ft_touches)
    field_tilt_passes = _share(ft_passes)

    # ── Passing rate (base passes, same exclusions as parse_passing) ──────────
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

    # ── PPDA (Opta definition) ────────────────────────────────────────────────
    # opposition passes outside the pressing team's own defensive third
    # (= opposition passes with opposition-frame x <= 66.6), divided by the
    # pressing team's defensive actions outside their own defensive third.
    opp_passes_allowed = {home_id: 0, away_id: 0}
    def_actions        = {home_id: 0, away_id: 0}
    for e in events:
        tid = e.get('teamId')
        if tid not in (home_id, away_id):
            continue
        other = away_id if tid == home_id else home_id
        etype = e['type']['displayName']
        x = _flt(e.get('x'), -1)
        if etype == 'Pass' and x <= FINAL_THIRD_MIN:
            opp_passes_allowed[other] += 1          # pass faced by the other side
        if etype in _PPDA_ACTIONS and x >= DEF_THIRD_MAX:
            def_actions[tid] += 1
    ppda = {tid: round(opp_passes_allowed[tid] / def_actions[tid], 2)
            if def_actions[tid] else None for tid in (home_id, away_id)}

    # ── Sequences + possessions ──────────────────────────────────────────────
    raw         = _build_sequences(sorted_events, home_id, away_id)
    possessions = _chain_possessions(raw)
    poss_id_of  = {}
    for p_id, (ptid, idxs) in enumerate(possessions):
        for i in idxs:
            poss_id_of[i] = p_id

    team_agg = {tid: defaultdict(float) for tid in (home_id, away_id)}
    sequence_rows = []

    for idx, (team_id, seq, dead_ball, next_event) in enumerate(raw):
        if team_id not in (home_id, away_id):
            continue
        if not _is_real_sequence(seq):
            continue

        a = team_agg[team_id]
        length   = len(seq)
        t_start  = _get_time(seq[0])
        duration = _get_time(seq[-1]) - t_start
        xs       = _seq_xs(seq)
        start_x  = xs[0] if xs else 50.0
        end_x    = xs[-1] if xs else 50.0
        outcome  = _seq_outcome(seq, dead_ball, next_event)
        if outcome is None:
            continue

        is_shot      = outcome in _SHOT_OUTCOMES
        is_open_play = not _is_restart(seq[0])
        n_passes     = sum(1 for e in seq if e['type']['displayName'] == 'Pass')
        reaches_ft   = any(x > FINAL_THIRD_MIN for x in xs)
        progress_m   = max(0.0, end_x - start_x) * PITCH_LENGTH / 100
        ends_box     = _ends_shot_or_box(seq)

        # ── Opta-style flags ──────────────────────────────────────────────
        is_ten_plus  = is_open_play and n_passes >= LONG_SEQ_MIN
        is_build_up  = is_ten_plus and ends_box
        is_direct    = (is_open_play and start_x <= 50.0 and ends_box
                        and _upfield_share(seq) >= 0.5 and length >= 2)
        start_y      = _flt(seq[0].get('y', 50))
        is_high_to   = (is_open_play
                        and _goal_dist_m(start_x, start_y) <= HIGH_TURNOVER_M
                        and idx in poss_id_of
                        and possessions[poss_id_of[idx]][1][0] == idx)
                        # high turnover counted at POSSESSION start, radial 40m (Opta)

        # ── Accumulate ────────────────────────────────────────────────────
        a['seq_count']        += 1
        a['seq_length_sum']   += length
        a['seq_duration_sum'] += duration
        a['seq_pass_sum']     += n_passes
        if is_open_play:
            a['op_seq_count']      += 1
            a['op_start_x_sum']    += start_x
            if start_x <= 50.0 and duration > 0:
                a['ds_progress_m'] += progress_m
                a['ds_time_s']     += duration
        if is_ten_plus:  a['ten_plus'] += 1
        if is_build_up:  a['build_up'] += 1
        if is_direct:    a['direct_attacks'] += 1
        if is_high_to:
            a['high_turnovers'] += 1
            if is_shot: a['high_to_shots'] += 1
        if is_shot:
            a['seq_shot_count']        += 1
            a['seq_shot_length_sum']   += length
            a['seq_shot_duration_sum'] += duration
            a['seq_shot_passes_sum']   += n_passes
        if reaches_ft and length >= 2:
            a['seq_attack_count']        += 1
            a['seq_attack_duration_sum'] += duration
            a['seq_attack_passes_sum']   += n_passes

        # Turnover accounting
        if outcome in ('turnover', 'dispossessed'):
            a['op_turnover_count'] += 1                       # strict
        if outcome in ('turnover', 'dispossessed', 'out_of_play'):
            a['possession_loss_count'] += 1                   # broad
        if outcome in ('foul_won', 'set_piece_won', 'foul_conceded', 'offside'):
            a['seq_setpiece_count'] += 1

        sequence_rows.append({
            'whoscored_match_id': whoscored_match_id,
            'team_id':            team_id,
            'possession_id':      poss_id_of.get(idx),
            'length':             length,
            'duration_s':         round(duration, 1),
            'start_x':            round(start_x, 1),
            'start_y':            round(_flt(seq[0].get('y', 50)), 1),
            'end_x':              round(end_x, 1),
            'end_y':              round(_flt(seq[-1].get('y', 50)), 1),
            'n_passes':           n_passes,
            'is_open_play':       is_open_play,
            'is_shot':            is_shot,
            'is_ten_plus_pass':   is_ten_plus,
            'is_build_up_attack': is_build_up,
            'is_direct_attack':   is_direct,
            'is_high_turnover':   is_high_to,
            'outcome':            outcome,
            'event_id_start':     seq[0].get('eventId'),
            'event_id_end':       seq[-1].get('eventId'),
            'player_id_start':    seq[0].get('playerId'),
            'player_id_end':      seq[-1].get('playerId'),
            'player_ids':         list(dict.fromkeys(
                                      e.get('playerId') for e in seq if e.get('playerId'))),
        })

    # ── Possession-level stats (incl. legacy counter-attack metric) ──────────
    poss_agg = {tid: defaultdict(float) for tid in (home_id, away_id)}
    for ptid, idxs in possessions:
        if ptid not in (home_id, away_id):
            continue
        p = poss_agg[ptid]
        ev_chain = [e for i in idxs for e in raw[i][1]]
        p['count'] += 1
        p['duration_sum'] += _get_time(ev_chain[-1]) - _get_time(ev_chain[0])
        p['seqs_sum'] += len(idxs)
        # legacy counter-attack: open-play start in own half, final third <=10s
        first_seq = raw[idxs[0]][1]
        start_x = _flt(first_seq[0].get('x', 50))
        if not _is_restart(first_seq[0]) and start_x <= 50.0:
            t0 = _get_time(first_seq[0])
            ft_e = next((e for e in ev_chain
                         if _flt(e.get('x', 0)) > FINAL_THIRD_MIN), None)
            if ft_e is not None and _get_time(ft_e) - t0 <= COUNTER_MAX_SECS:
                p['counter_count'] += 1
                if any(raw[i][1][-1].get('isShot') for i in idxs):
                    p['counter_shot_count'] += 1

    # ── Build team rows ───────────────────────────────────────────────────────
    team_rows = []
    for tid in (home_id, away_id):
        a, p = team_agg[tid], poss_agg[tid]
        sc   = a['seq_count'];  shc = a['seq_shot_count']
        atk  = a['seq_attack_count']; ops = a['op_seq_count']
        pc   = p['count']

        def _avg(num, den, nd=1):
            return round(num / den, nd) if den else None

        team_rows.append({
            'whoscored_match_id':        whoscored_match_id,
            'team_id':                   tid,
            # Possession share / rate
            'passing_rate':              passing_rate[tid],
            'passes_accurate':           acc_passes[tid],
            'possession_minutes':        round(poss_minutes[tid], 1),
            # Field tilt
            'field_tilt_pct':            field_tilt[tid],
            'field_tilt_passes_pct':     field_tilt_passes[tid],
            'final_third_touches':       ft_touches[tid],
            # Pressing
            'ppda':                      ppda[tid],
            # ── Sequence level (Opta-comparable) ──
            'sequences_total':           int(sc),
            'open_play_sequences':       int(ops),
            'avg_sequence_time_s':       _avg(a['seq_duration_sum'], sc),
            'avg_sequence_length':       _avg(a['seq_length_sum'], sc),
            'passes_per_sequence':       _avg(a['seq_pass_sum'], sc),
            'start_distance_m':          _avg(a['op_start_x_sum'] * PITCH_LENGTH / 100, ops),
            'direct_speed_m_s':          _avg(a['ds_progress_m'], a['ds_time_s'], 2),
            'ten_plus_pass_sequences':   int(a['ten_plus']),
            'build_up_attacks':          int(a['build_up']),
            'direct_attacks':            int(a['direct_attacks']),
            'high_turnovers':            int(a['high_turnovers']),
            'high_turnover_shots':       int(a['high_to_shots']),
            # Shot-ending sequences
            'shot_sequences':            int(shc),
            'shot_seq_pct':              _avg(shc * 100, sc),
            'avg_shot_seq_length':       _avg(a['seq_shot_length_sum'], shc),
            'avg_shot_seq_duration_s':   _avg(a['seq_shot_duration_sum'], shc),
            'avg_shot_seq_passes':       _avg(a['seq_shot_passes_sum'], shc),
            # Attack speed (sequences reaching final third)
            'attack_sequences':          int(atk),
            'avg_attack_duration_s':     _avg(a['seq_attack_duration_sum'], atk),
            'avg_attack_passes':         _avg(a['seq_attack_passes_sum'], atk),
            # ── Possession level ──
            'possessions_total':         int(pc),
            'avg_possession_duration_s': _avg(p['duration_sum'], pc),
            'avg_sequences_per_possession': _avg(p['seqs_sum'], pc, 2),
            # Counter-attacks (legacy metric, now possession-level + open play)
            'counter_attack_sequences':  int(p['counter_count']),
            'counter_attack_shot_pct':   _avg(p['counter_shot_count'] * 100, p['counter_count']),
            # ── Turnovers ──
            'open_play_turnover_pct':    _avg(a['op_turnover_count'] * 100, sc),
            'possession_loss_pct':       _avg(a['possession_loss_count'] * 100, sc),
            'set_piece_end_pct':         _avg(a['seq_setpiece_count'] * 100, sc),
        })

    return {
        'team':      team_rows,
        'sequences': sequence_rows,
    }


if __name__ == "__main__":
    import json
    path = './1903468.json'
    with open(path, "r", encoding='utf-8') as fp:
        match_data = json.load(fp)

    result = parse_possession_advanced(match_data, 1729476)
    with open("./result_advanced.json", "w", encoding='utf-8') as fp:
        json.dump(result, fp, indent=4)