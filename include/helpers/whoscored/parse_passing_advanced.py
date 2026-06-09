"""
parse_passing_advanced.py
-------------------------
Derived and advanced passing metrics, computed from WhoScored matchCentreData.

This file is a companion to parse_passing.py which handles standard counts.
All metrics here are either spatially derived, model-based, or structural.

DEFINITIONS (Opta / industry standard)
---------------------------------------
Progressive pass
    A COMPLETED pass that starts in the attacking two-thirds (x > 33.3),
    and reduces the Euclidean distance to goal by at least 25%.
    distance_to_goal = sqrt((100 - x)^2 + (50 - y)^2)
    Condition: dist_to_goal(end) < 0.75 * dist_to_goal(start)
    Source: Opta Analyst definitions.

Final third pass
    A COMPLETED pass that starts outside the final third (x < 66.6)
    and ends inside it (end_x >= 66.6). Passes entirely within the
    final third are excluded — this measures entries, not circulation.

Penalty area pass
    A COMPLETED pass where end_x >= 83 AND 21 <= end_y <= 79.

Key pass (Opta)
    A pass or touch (KeyPass qualifier on Pass or BallTouch events) that
    leads directly to a shot that does NOT result in a goal. Excludes
    the touch/pass that directly preceded a goal (that is the assist).
    Chance Created = Key Passes + Assists.

Passes into channels / switches
    Switch of play: base pass with length > 32 AND lateral distance > 25.

PPDA (Opta)
    Opposition passes OUTSIDE their own defensive third (x > 33.3 from
    their perspective, i.e. the pressing team's attacking 66.6) divided
    by the pressing team's defensive actions outside THEIR own defensive
    third (x > 33.3): tackles + interceptions + fouls + challenges +
    blocked passes. Lower = more aggressive press.
    Source: Opta Analyst definitions.

Field tilt
    Team's touches in the final third (x >= 66.6) divided by total
    touches in the final third by both teams. Pure territorial dominance.

xT (Expected Threat)
    Karun Singh 12x8 Markov chain grid. Value of end zone minus start
    zone per base pass. Total xT = sum of all xT_added per team.

Passing speed (team)
    Within each possession sequence, measure time gaps between consecutive
    base passes by DIFFERENT players. Average = team circulation tempo.
    Quick pass % = % of gaps <= 2s. Slow pass % = % of gaps > 5s.

Player hold time
    Per player: average time between their own previous touch event and
    their pass event. Measures how long a player holds the ball before
    releasing — distinct from team passing speed.

Pass map data
    Player average positions (mean x, y of all touch events).
    Player-to-player directed pass counts within base passes.
    Receiver = next event in time order by a teammate in same sequence.

Levels: team and player (noted per metric).
"""

import math
from collections import defaultdict


# ── Constants ─────────────────────────────────────────────────────────────────

PASS_EXCLUDE_IDS = {2, 107, 123}   # Cross, ThrowIn, KeeperThrow

# Dead ball qualifier IDs — used to split open play vs set piece counts
# 5=FreekickTaken, 6=CornerTaken, 9=Penalty, 107=ThrowIn,
# 123=KeeperThrow, 124=GoalKick, 241=IndirectFreekickTaken
DEAD_BALL_IDS = {5, 6, 9, 107, 123, 124, 241}

# xT grid — Karun Singh (2018), 12 rows (x-axis) × 8 cols (y-axis)
XT_GRID = [
    [0.00638,0.00349,0.00634,0.00938,0.01093,0.01073,0.00634,0.00316],
    [0.00779,0.00573,0.00674,0.01217,0.01402,0.01134,0.00786,0.00463],
    [0.00911,0.00956,0.01128,0.01670,0.02094,0.01775,0.01087,0.00736],
    [0.01117,0.01035,0.01914,0.02845,0.03339,0.02955,0.01840,0.01028],
    [0.01740,0.01880,0.02867,0.04685,0.06168,0.05055,0.02936,0.01740],
    [0.01958,0.02837,0.05017,0.09666,0.14777,0.10150,0.05515,0.02609],
    [0.03229,0.07104,0.14790,0.32686,0.37779,0.31918,0.14052,0.05785],
    [0.03985,0.11791,0.27373,0.55862,0.70388,0.56020,0.25651,0.11250],
    [0.04089,0.15490,0.35700,0.73635,0.88798,0.68555,0.34514,0.12998],
    [0.07092,0.24042,0.47735,0.84972,0.95987,0.84515,0.44834,0.22405],
    [0.10536,0.33450,0.66219,0.92441,0.99408,0.92348,0.61396,0.29224],
    [0.19017,0.50016,0.80600,0.97050,0.99840,0.95988,0.68517,0.37805],
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _has(event, name):
    return any(q['type']['displayName'] == name
               for q in event.get('qualifiers', []))

def _has_qid(event, qid):
    return any(q.get('type', {}).get('value') == qid
               for q in event.get('qualifiers', []))

def _get(event, name):
    for q in event.get('qualifiers', []):
        if q['type']['displayName'] == name:
            return q.get('value')
    return None

def _flt(v):
    try: return float(v)
    except: return 0.0

def _acc(event):
    return event.get('outcomeType', {}).get('displayName') == 'Successful'

def _is_base_pass(event):
    return not any(q.get('type', {}).get('value') in PASS_EXCLUDE_IDS
                   for q in event.get('qualifiers', []))

def _dist_to_goal(x, y):
    """Euclidean distance from (x, y) to centre of opponent goal (100, 50)."""
    return math.sqrt((100 - _flt(x)) ** 2 + (50 - _flt(y)) ** 2)

def _xt(x, y):
    col = min(int(_flt(y) / 100 * 8), 7)
    row = min(int(_flt(x) / 100 * 12), 11)
    return XT_GRID[row][col]

def _time_seconds(event):
    return event.get('expandedMinute', 0) * 60 + (event.get('second') or 0)


# ── Sequence builder ──────────────────────────────────────────────────────────

def _build_sequences(events, home_id, away_id):
    """
    Returns list of {'team': team_id, 'events': [...]} dicts.
    A sequence is a consecutive run of events by the same team, sorted by time.
    Opta definition: ended by defensive action, stoppage, or shot.
    """
    skip = {'Start', 'End', 'FormationSet', 'FormationChange',
            'SubstitutionOn', 'SubstitutionOff'}
    sorted_evs = sorted(
        [e for e in events
         if e.get('teamId') in (home_id, away_id)
         and e['type']['displayName'] not in skip],
        key=lambda e: (_flt(e.get('expandedMinute', 0)), _flt(e.get('second', 0)))
    )
    sequences, cur_seq, cur_team = [], [], None
    for e in sorted_evs:
        tid = e.get('teamId')
        if tid != cur_team:
            if cur_seq:
                sequences.append({'team': cur_team, 'events': cur_seq})
            cur_seq, cur_team = [e], tid
        else:
            cur_seq.append(e)
    if cur_seq:
        sequences.append({'team': cur_team, 'events': cur_seq})
    return sequences


# ── Pass map helpers ──────────────────────────────────────────────────────────

def _get_receiver(pass_event, seq_events):
    """
    Find the receiver of a pass: the next event by a different teammate
    in the same possession sequence. Returns player_id or None.
    """
    passer_id  = pass_event.get('playerId')
    pass_time  = _time_seconds(pass_event)
    for e in seq_events:
        if _time_seconds(e) <= pass_time:
            continue
        if e.get('playerId') and e.get('playerId') != passer_id:
            return e.get('playerId')
    return None


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_passing_advanced(data: dict, whoscored_match_id: int) -> dict:
    """
    Compute derived and advanced passing metrics.

    Returns:
        {
            'team':        [home_row, away_row]  — team-level advanced stats,
            'player':      [...]                 — player-level advanced stats,
            'pass_map':    [...]                 — per-pass rows for visualisation,
            'player_positions': [...]            — avg position per player,
            'pass_network': [...]                — directed player-to-player counts,
        }
    """
    events   = data.get('events', [])
    home_id  = data['home']['teamId']
    away_id  = data['away']['teamId']
    names    = data.get('playerIdNameDictionary', {})

    sorted_events = sorted(
        events,
        key=lambda e: (_flt(e.get('expandedMinute', 0)), _flt(e.get('second', 0)))
    )

    sequences = _build_sequences(events, home_id, away_id)

    # Index events by eventId for assist chain lookups
    event_by_id = {e.get('eventId'): e for e in events}

    # ── Identify base passes with receiver ────────────────────────────────────
    # Build a lookup: sequence events per (team, sequence_index)
    seq_lookup = {}
    for i, seq in enumerate(sequences):
        for e in seq['events']:
            seq_lookup[e.get('eventId')] = (seq['team'], i)

    # Build pass rows enriched with receiver
    base_pass_rows = []
    for seq in sequences:
        seq_events = seq['events']
        for e in seq_events:
            if e.get('type', {}).get('value') != 1:
                continue
            if not _is_base_pass(e):
                continue
            x     = _flt(e.get('x', 0))
            y     = _flt(e.get('y', 50))
            end_x = _flt(_get(e, 'PassEndX') or e.get('endX', x))
            end_y = _flt(_get(e, 'PassEndY') or e.get('endY', y))
            acc   = _acc(e)
            # v_gain scaled to metres (WhoScored 0-100 coords, pitch = 105m long)
            v_gain = (end_x - x) * 1.05

            # Progressive pass (Opta): completed, starts in att 2/3, 25% closer to goal
            dist_start = _dist_to_goal(x, y)
            dist_end   = _dist_to_goal(end_x, end_y)
            is_progressive = (
                acc
                and x > 33.3
                and dist_end < 0.75 * dist_start
            )

            # Final third entry: crosses into the final third (no accuracy filter)
            # Note: this flag is on base_pass_rows only (excl Cross/TI/KT)
            # Team-level passes_entering_final_third is computed separately from raw events
            is_final_third = x < 66.6 and end_x >= 66.6

            # Penalty area pass: completed, ends in box
            is_penalty_area = acc and end_x >= 83 and 21 <= end_y <= 79

            # Switch of play
            length = _flt(_get(e, 'Length'))
            is_switch = length > 32 and abs(y - end_y) > 25

            # Open play flag — no dead ball qualifiers
            q_ids_set = {q.get('type',{}).get('value') for q in e.get('qualifiers',[])}
            is_open_play = not bool(q_ids_set & DEAD_BALL_IDS)

            # xT
            xt_start = _xt(x, y)
            xt_end   = _xt(end_x, end_y)
            xt_added = xt_end - xt_start

            # Receiver
            receiver_id = _get_receiver(e, seq_events)

            base_pass_rows.append({
                'event_id':       e.get('eventId'),
                'player_id':      e.get('playerId'),
                'player_name':    names.get(str(e.get('playerId'))),
                'team_id':        e.get('teamId'),
                'minute':         e.get('minute'),
                'second':         e.get('second'),
                'expanded_minute':e.get('expandedMinute'),
                'period':         e.get('period', {}).get('displayName'),
                'x': x, 'y': y, 'end_x': end_x, 'end_y': end_y,
                'length':         length,
                'accurate':       acc,
                'v_gain':         v_gain,
                'dist_to_goal_start': round(dist_start, 2),
                'dist_to_goal_end':   round(dist_end, 2),
                'is_progressive':     is_progressive,
                'is_final_third':     is_final_third,
                'is_penalty_area':    is_penalty_area,
                'is_switch':          is_switch,
                'is_open_play':       is_open_play,
                'xt_start':           round(xt_start, 6),
                'xt_end':             round(xt_end, 6),
                'xt_added':           round(xt_added, 6),
                'receiver_id':        receiver_id,
                'receiver_name':      names.get(str(receiver_id)) if receiver_id else None,
            })

    # ── Identify goal-leading touches/passes (assists) ─────────────────────────
    # These are excluded from key pass count (Opta: key pass = leads to shot NOT goal)
    assist_event_ids = set()
    for e in events:
        if (e['type']['displayName'] == 'Goal'
                and _has_qid(e, 29)):          # Assisted qualifier
            rel_id = _get(e, 'RelatedEventId')
            if rel_id:
                assist_event_ids.add(int(float(rel_id)))

    # ── Team-level metrics ─────────────────────────────────────────────────────
    team_rows = []
    for team_id in (home_id, away_id):
        rows = [r for r in base_pass_rows if r['team_id'] == team_id]
        acc_rows = [r for r in rows if r['accurate']]
        n = len(rows)

        # 1. Spatial / progression
        progressive   = sum(1 for r in rows if r['is_progressive'])
        penalty_area  = sum(1 for r in rows if r['is_penalty_area'])

        # passes_entering_final_third: computed from raw events (not base_pass_rows)
        # Pass (type 1) + OffsidePass (type 2), start_x < 66.6 → end_x >= 66.6
        # Excludes KeeperThrow (123) only. All outcomes count (acc + inacc).
        # NOTE: undercounts vs Opta/SS by 0-2 (no carry data in WhoScored)
        passes_entering_ft = 0
        for e in events:
            if e.get('teamId') != team_id:
                continue
            if e.get('type',{}).get('value') not in (1, 2):  # Pass or OffsidePass
                continue
            q_ids_e = {q.get('type',{}).get('value') for q in e.get('qualifiers',[])}
            if 123 in q_ids_e:
                continue
            start_x_e = _flt(e.get('x', 0))
            end_x_e   = _flt(_get(e, 'PassEndX'))
            if end_x_e == 0.0:
                continue
            if start_x_e < 66.6 and end_x_e >= 66.6:
                passes_entering_ft += 1

        lengths = [r['length'] for r in rows if r['length']]
        avg_len = round(sum(lengths) / len(lengths), 2) if lengths else None

        # Average pass length via Euclidean (for comparison)
        euclid_lens = [
            math.sqrt((r['end_x'] - r['x'])**2 + (r['end_y'] - r['y'])**2)
            for r in acc_rows
        ]
        avg_len_euclid = round(sum(euclid_lens) / len(euclid_lens), 2) if euclid_lens else None

        v_gains = [r['v_gain'] for r in rows]
        avg_vgain = round(sum(v_gains) / len(v_gains), 2) if v_gains else None

        # Forward / backward / lateral
        fwd_pct  = round(sum(1 for v in v_gains if v > 0)  / n * 100, 2) if n else 0
        back_pct = round(sum(1 for v in v_gains if v < 0)  / n * 100, 2) if n else 0
        lat_pct  = round(sum(1 for v in v_gains if v == 0) / n * 100, 2) if n else 0

        # Directness index
        dir_idx = round(progressive / n, 4) if n else 0

        # Switches
        switches = sum(1 for r in rows if r['is_switch'])

        # 2. xT
        xt_total    = round(sum(r['xt_added'] for r in rows), 4)
        xt_positive = round(sum(r['xt_added'] for r in rows if r['xt_added'] > 0), 4)
        xt_per_pass = round(xt_total / n, 6) if n else None

        # 3. Key passes (Opta: leads to shot NOT a goal)
        #    KeyPass qualifier on Pass or BallTouch, excluding assist events
        key_passes_opta = sum(
            1 for e in events
            if e.get('teamId') == team_id
            and e.get('type', {}).get('value') in (1, 61)
            and _has(e, 'KeyPass')
            and e.get('eventId') not in assist_event_ids
        )
        assists = sum(
            1 for e in events
            if e['type']['displayName'] == 'Goal'
            and e.get('teamId') == team_id
            and _has_qid(e, 29)
        )
        chances_created = key_passes_opta + assists

        # Open play variants — exclude dead ball situations
        def _is_open_play_event(e):
            return not any(q.get('type',{}).get('value') in DEAD_BALL_IDS
                           for q in e.get('qualifiers', []))

        key_passes_open_play = sum(
            1 for e in events
            if e.get('teamId') == team_id
            and e.get('type', {}).get('value') in (1, 61)
            and _has(e, 'KeyPass')
            and e.get('eventId') not in assist_event_ids
            and _is_open_play_event(e)
        )
        assists_open_play = sum(
            1 for e in events
            if e['type']['displayName'] == 'Goal'
            and e.get('teamId') == team_id
            and _has_qid(e, 29)
            and (lambda rel: _is_open_play_event(event_by_id.get(int(float(rel)), {}))
                 if rel else False)(_get(e, 'RelatedEventId'))
        )
        chances_created_open_play = key_passes_open_play + assists_open_play

        big_chances_created_all = sum(
            1 for e in events
            if e.get('teamId') == team_id
            and e.get('type', {}).get('value') == 1
            and _has(e, 'BigChanceCreated')
        )
        big_chances_created_open_play = sum(
            1 for e in events
            if e.get('teamId') == team_id
            and e.get('type', {}).get('value') == 1
            and _has(e, 'BigChanceCreated')
            and _is_open_play_event(e)
        )

        # Crosses open play (from all team pass events, not base passes)
        all_team_passes = [e for e in events
                           if e.get('type',{}).get('value') == 1
                           and e.get('teamId') == team_id]
        crosses_total     = [p for p in all_team_passes if _has_qid(p, 2)]
        crosses_open      = [p for p in crosses_total if _is_open_play_event(p)]
        crosses_open_att  = len(crosses_open)
        crosses_open_acc  = sum(1 for p in crosses_open if _acc(p))

        # Corner deliveries open play
        corners_total    = [p for p in all_team_passes if _has_qid(p, 6)]
        corners_open     = [p for p in corners_total if _is_open_play_event(p)]
        # Corner kicks are always dead ball by definition — open play = 0
        # But we store for completeness
        corners_open_att = len(corners_open)

        # passes_in_final_third: Pass (type 1), end_x >= 66.6, excl {2,107,123}
        # (separate from base_pass_rows which have the entries definition)
        passes_in_ft_att = 0
        passes_in_ft_acc = 0
        passes_in_ft_op_att = 0
        passes_in_ft_op_acc = 0
        for p in all_team_passes:
            q_ids_p = {q.get('type',{}).get('value') for q in p.get('qualifiers',[])}
            if q_ids_p & {2, 107, 123}:
                continue
            end_x_p = _flt(_get(p, 'PassEndX'))
            if end_x_p == 0.0:
                continue
            if end_x_p >= 66.6:
                passes_in_ft_att += 1
                is_acc_p = _acc(p)
                if is_acc_p: passes_in_ft_acc += 1
                if _is_open_play_event(p):
                    passes_in_ft_op_att += 1
                    if is_acc_p: passes_in_ft_op_acc += 1

        # 4. PPDA (Opta)
        # Numerator: opponent passes outside pressing team's own defensive third
        # i.e. in the opponent's attacking 66.6+ (from the opponent's perspective)
        # = passes where x > 33.3 (attacker's frame), which means from pressing
        # team's view, the opponent is in the pressing team's half/final third
        # Opta: "opposition passes outside pressing team's own defensive third"
        # = opponent passes where the ball is at x > 33.3 FROM THE DEFENDING SIDE
        # In WhoScored coords, both teams attack toward x=100, so we flip:
        # opponent's x in their own frame = 100 - their event x
        # "outside pressing team's defensive third" = not in pressing team's def 3rd
        # = passes that happen when the ball is at x > 33.3 (pressing team's coords)
        opp_id = away_id if team_id == home_id else home_id
        opp_passes_in_zone = sum(
            1 for e in events
            if e.get('type', {}).get('value') == 1
            and e.get('teamId') == opp_id
            and _flt(e.get('x', 0)) > 33.3   # outside pressing team's defensive third
        )
        def_actions = sum(
            1 for e in events
            if e.get('teamId') == team_id
            and e['type']['displayName'] in
            ('Tackle', 'Interception', 'Foul', 'Challenge')
            and _flt(e.get('x', 0)) > 33.3
        ) + sum(
            1 for e in events
            if e.get('type', {}).get('value') == 74   # BlockedPass
            and e.get('teamId') == team_id
            and _flt(e.get('x', 0)) > 33.3
        )
        ppda = round(opp_passes_in_zone / def_actions, 2) if def_actions else None

        # 5. Field tilt
        ft_touches_team = sum(
            1 for e in events
            if e.get('teamId') == team_id
            and e.get('isTouch')
            and _flt(e.get('x', 0)) >= 66.6
        )
        ft_touches_opp = sum(
            1 for e in events
            if e.get('teamId') == opp_id
            and e.get('isTouch')
            and _flt(e.get('x', 0)) >= 66.6
        )
        ft_total = ft_touches_team + ft_touches_opp
        field_tilt_pct = round(ft_touches_team / ft_total * 100, 2) if ft_total else None

        # 6. Passing speed (team circulation tempo)
        release_times = []
        for seq in sequences:
            if seq['team'] != team_id:
                continue
            seq_passes = [
                e for e in seq['events']
                if e['type']['displayName'] == 'Pass' and _is_base_pass(e)
            ]
            for i in range(1, len(seq_passes)):
                p1, p2 = seq_passes[i - 1], seq_passes[i]
                if p1.get('playerId') == p2.get('playerId'):
                    continue
                gap = _time_seconds(p2) - _time_seconds(p1)
                if 0 < gap <= 10:
                    release_times.append(gap)

        avg_release   = round(sum(release_times) / len(release_times), 2) if release_times else None
        quick_pass_pct = round(sum(1 for t in release_times if t <= 2) / len(release_times) * 100, 2) if release_times else None
        slow_pass_pct  = round(sum(1 for t in release_times if t > 5)  / len(release_times) * 100, 2) if release_times else None

        team_rows.append({
            'whoscored_match_id':   whoscored_match_id,
            'team_id':              team_id,
            # Spatial / progression
            'progressive_passes':              progressive,
            'passes_entering_final_third':     passes_entering_ft,
            'passes_in_final_third':           passes_in_ft_att,
            'passes_in_final_third_accurate':  passes_in_ft_acc,
            'passes_in_final_third_open_play': passes_in_ft_op_att,
            'passes_in_final_third_op_accurate': passes_in_ft_op_acc,
            'penalty_area_passes':             penalty_area,
            'switches_of_play':                switches,
            'avg_pass_length':                 avg_len,
            'avg_pass_length_euclid':          avg_len_euclid,
            'avg_vertical_gain':               avg_vgain,
            # Directness
            'forward_pass_pct':     fwd_pct,
            'backward_pass_pct':    back_pct,
            'lateral_pass_pct':     lat_pct,
            'directness_index':     dir_idx,
            # xT
            'xt_total':             xt_total,
            'xt_positive':          xt_positive,
            'xt_per_pass':          xt_per_pass,
            # Chance creation (Opta definitions)
            'key_passes':                     key_passes_opta,
            'key_passes_open_play':           key_passes_open_play,
            'assists':                        assists,
            'assists_open_play':              assists_open_play,
            'chances_created':                chances_created,
            'chances_created_open_play':      chances_created_open_play,
            'big_chances_created':            big_chances_created_all,
            'big_chances_created_open_play':  big_chances_created_open_play,
            # Crosses (open play split)
            'crosses_open_play_attempted':    crosses_open_att,
            'crosses_open_play_accurate':     crosses_open_acc,
            # Corner deliveries (open play split — always 0 by definition)
            'corner_deliveries_open_play':    corners_open_att,
            # PPDA
            'ppda':                 ppda,
            # Field tilt
            'field_tilt_pct':       field_tilt_pct,
            # Passing speed
            'avg_release_seconds':  avg_release,
            'quick_pass_pct':       quick_pass_pct,
            'slow_pass_pct':        slow_pass_pct,
        })

    # ── Player-level metrics ───────────────────────────────────────────────────
    player_rows = []
    player_pass_map = defaultdict(list)
    for r in base_pass_rows:
        player_pass_map[(r['team_id'], r['player_id'])].append(r)

    for (team_id, player_id), rows in player_pass_map.items():
        if not player_id:
            continue
        acc_rows = [r for r in rows if r['accurate']]
        n = len(rows)

        lengths  = [r['length'] for r in rows if r['length']]
        v_gains  = [r['v_gain'] for r in rows]
        xt_vals  = [r['xt_added'] for r in rows]

        progressive  = sum(1 for r in rows if r['is_progressive'])
        final_third  = sum(1 for r in rows if r['is_final_third'])
        penalty_area = sum(1 for r in rows if r['is_penalty_area'])

        # Key passes (Opta)
        kp = sum(
            1 for e in events
            if e.get('teamId') == team_id
            and e.get('playerId') == player_id
            and e.get('type', {}).get('value') in (1, 61)
            and _has(e, 'KeyPass')
            and e.get('eventId') not in assist_event_ids
        )
        # Player assists — resolve via RelatedEventId on Goal events
        player_assists = 0
        for e in events:
            if (e['type']['displayName'] == 'Goal'
                    and e.get('teamId') == team_id
                    and _has_qid(e, 29)):
                rel_id = _get(e, 'RelatedEventId')
                if rel_id:
                    assist_evt = event_by_id.get(int(float(rel_id)))
                    if assist_evt and assist_evt.get('playerId') == player_id:
                        player_assists += 1

        # Player hold time — avg seconds between player's previous touch and their pass
        player_touch_times = sorted(
            [_time_seconds(e) for e in events
             if e.get('playerId') == player_id
             and e.get('teamId') == team_id
             and e.get('isTouch')],
        )
        player_pass_times = sorted(
            [_time_seconds(e) for e in events
             if e.get('playerId') == player_id
             and e.get('teamId') == team_id
             and e.get('type', {}).get('value') == 1
             and _is_base_pass(e)]
        )
        hold_times = []
        for pt in player_pass_times:
            prev_touches = [t for t in player_touch_times if t < pt]
            if prev_touches:
                gap = pt - max(prev_touches)
                if 0 < gap <= 15:
                    hold_times.append(gap)

        player_rows.append({
            'whoscored_match_id':          whoscored_match_id,
            'team_id':                     team_id,
            'player_id':                   player_id,
            'player_name':                 names.get(str(player_id)),
            'progressive_passes':          progressive,
            'passes_entering_final_third': final_third,
            'penalty_area_passes':         penalty_area,
            'avg_pass_length':             round(sum(lengths) / len(lengths), 2) if lengths else None,
            'avg_vertical_gain':           round(sum(v_gains) / len(v_gains), 2) if v_gains else None,
            'forward_pass_pct':            round(sum(1 for v in v_gains if v > 0) / n * 100, 2) if n else 0,
            'backward_pass_pct':           round(sum(1 for v in v_gains if v < 0) / n * 100, 2) if n else 0,
            'xt_total':                    round(sum(xt_vals), 4),
            'xt_positive':                 round(sum(v for v in xt_vals if v > 0), 4),
            'xt_per_pass':                 round(sum(xt_vals) / n, 6) if n else None,
            'key_passes':                  kp,
            'assists':                     player_assists,
            'chances_created':             kp + player_assists,
            'avg_hold_seconds':            round(sum(hold_times) / len(hold_times), 2) if hold_times else None,
        })

    # ── Player average positions ───────────────────────────────────────────────
    touch_positions = defaultdict(list)
    for e in events:
        if e.get('isTouch') and e.get('playerId') and e.get('teamId') in (home_id, away_id):
            touch_positions[(e['teamId'], e['playerId'])].append(
                (_flt(e.get('x', 0)), _flt(e.get('y', 50)))
            )

    player_positions = []
    for (team_id, player_id), positions in touch_positions.items():
        xs = [p[0] for p in positions]
        ys = [p[1] for p in positions]
        player_positions.append({
            'whoscored_match_id': whoscored_match_id,
            'team_id':            team_id,
            'player_id':          player_id,
            'player_name':        names.get(str(player_id)),
            'avg_x':              round(sum(xs) / len(xs), 2),
            'avg_y':              round(sum(ys) / len(ys), 2),
            'touch_count':        len(positions),
        })

    # ── Pass network (directed player-to-player counts) ───────────────────────
    # Uses ALL accurate pass events (no base pass filter) — matches Opta's
    # pass network definition which includes GK distributions, long kicks etc.
    pair_counts = defaultdict(int)
    for seq in sequences:
        seq_events = seq['events']
        for e in seq_events:
            if e.get('type', {}).get('value') != 1:
                continue
            if not _acc(e):   # accurate passes only
                continue
            passer_id   = e.get('playerId')
            receiver_id = _get_receiver(e, seq_events)
            if passer_id and receiver_id:
                pair_counts[(e.get('teamId'), passer_id, receiver_id)] += 1

    pass_network = [
        {
            'whoscored_match_id': whoscored_match_id,
            'team_id':            team_id,
            'passer_id':          passer_id,
            'passer_name':        names.get(str(passer_id)),
            'receiver_id':        receiver_id,
            'receiver_name':      names.get(str(receiver_id)),
            'pass_count':         count,
        }
        for (team_id, passer_id, receiver_id), count in pair_counts.items()
    ]

    return {
        'team':             team_rows,
        'player':           player_rows,
        'pass_map':         base_pass_rows,
        'player_positions': player_positions,
        'pass_network':     pass_network,
    }



if __name__ == "__main__":
    import json
    path = './1903468.json'
    with open(path, "r", encoding='utf-8') as file:
        file = json.load(file)

    result = parse_passing_advanced(file, 1729476)
    with open("./result_advanced.json", "w", encoding='utf-8') as fp:
        json.dump(result , fp, ensure_ascii=False)
