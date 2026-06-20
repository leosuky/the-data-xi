"""
parse_passing.py
----------------
Parses WhoScored matchCentreData into team-level passing stats.

DEFINITIONS
-----------
base_passes
    Pass events (type 1) excluding qualifier IDs {2, 107, 123}.
    2   = Cross      → tracked separately
    107 = ThrowIn    → tracked separately
    123 = KeeperThrow → tracked separately

    This is the industry-standard pass baseline (matches Fotmob / Sofascore /
    WhoScored stats field). Every volume metric and pass-type breakdown runs
    on base_passes only. Verified 20/20 across 10 games.

Corner deliveries
    Pass events with CornerTaken qualifier (QID 6). Always carry Cross (QID 2)
    so they are excluded from base_passes. Tracked independently.

Key passes
    KeyPass qualifier on ANY event type (Pass or BallTouch). No exclusion
    filter — a cross or corner delivery that leads to a shot is still a key
    pass. Verified: Man City 16 = 14 Pass + 2 BallTouch.

Assists
    Goal events with Assisted qualifier (QID 29). Follow RelatedEventId to the
    assisting event. Count those assisting events per team. Works for both pass
    assists and touch assists. Verified: both Man City assists correctly
    resolved (Aké BallTouch + De Bruyne Pass).

Big chances created
    BigChanceCreated qualifier on Pass events. No exclusion filter.

All stats from raw events only. No pre-recorded stats fields used.
Player-level and derived/advanced stats omitted for now — team stats first.
"""

from collections import defaultdict


# ── Constants ─────────────────────────────────────────────────────────────────

PASS_EXCLUDE_IDS = {2, 107, 123}   # Cross, ThrowIn, KeeperThrow


# ── Helpers ───────────────────────────────────────────────────────────────────

def _has(event, name):
    return any(q['type']['displayName'] == name
               for q in event.get('qualifiers', []))

def _has_qid(event, qid):
    return any(q.get('type', {}).get('value') == qid
               for q in event.get('qualifiers', []))

def _get_qid_val(event, qid):
    for q in event.get('qualifiers', []):
        if q.get('type', {}).get('value') == qid:
            return q.get('value')
    return None

def _acc(event):
    return event.get('outcomeType', {}).get('displayName') == 'Successful'

def _is_base_pass(event):
    return not any(q.get('type', {}).get('value') in PASS_EXCLUDE_IDS
                   for q in event.get('qualifiers', []))

def _flt(v):
    try: return float(v)
    except: return 0.0

def _get_qname_val(event, name):
    for q in event.get('qualifiers', []):
        if q['type']['displayName'] == name:
            return q.get('value')
    return None


# ── Main parser ───────────────────────────────────────────────────────────────

def parse_passing(data: dict, whoscored_match_id: int) -> dict:
    """
    Returns {'team': [home_row, away_row]}.
    """
    events   = data.get('events', [])
    home_id  = data['home']['teamId']
    away_id  = data['away']['teamId']

    # Index all events by eventId for assist resolution
    event_by_id = {e.get('eventId'): e for e in events}

    # Sort events by time once — used for sequence building
    sorted_events = sorted(
        events,
        key=lambda e: (e.get('expandedMinute', 0), e.get('second', 0))
    )

    team_stats = []

    for team_id in (home_id, away_id):

        # ── Base passes — foundation for all volume metrics ───────────────────
        base_passes = [
            e for e in events
            if e.get('type', {}).get('value') == 1
            and e.get('teamId') == team_id
            and _is_base_pass(e)
        ]
        base_acc = [p for p in base_passes if _acc(p)]

        # ── All pass events for this team (for separated metrics) ─────────────
        all_passes = [
            e for e in events
            if e.get('type', {}).get('value') == 1
            and e.get('teamId') == team_id
        ]

        # ── 1. Volume ─────────────────────────────────────────────────────────
        passes_total      = len(base_passes)
        passes_accurate   = len(base_acc)
        passes_inaccurate = passes_total - passes_accurate
        pass_accuracy_pct = round(passes_accurate / passes_total * 100, 2) if passes_total else 0

        # ── 2. Pass type (base passes only) ───────────────────────────────────
        longballs     = [p for p in base_passes if _has(p, 'Longball')]
        lb_acc        = sum(1 for p in longballs if _acc(p))
        throughballs  = sum(1 for p in base_passes if _has(p, 'Throughball'))
        head_passes   = sum(1 for p in base_passes if _has(p, 'HeadPass'))
        layoffs       = sum(1 for p in base_passes if _has(p, 'LayOff'))

        # Length bands — from base passes, must sum to passes_total
        passes_short     = sum(1 for p in base_passes if _flt(_get_qid_val(p, 212) or _get_qname_val(p, 'Length')) < 10)
        passes_medium    = sum(1 for p in base_passes if 10 <= _flt(_get_qid_val(p, 212) or _get_qname_val(p, 'Length')) <= 32)
        passes_long_band = sum(1 for p in base_passes if _flt(_get_qid_val(p, 212) or _get_qname_val(p, 'Length')) > 32)

        # ── 3. Set pieces (within base passes) ────────────────────────────────
        freekick_passes  = sum(1 for p in base_passes if _has_qid(p, 5))    # FreekickTaken
        indirect_fk      = sum(1 for p in base_passes if _has_qid(p, 241))  # IndirectFreekickTaken
        goalkick_passes  = sum(1 for p in base_passes if _has_qid(p, 124))  # GoalKick

        # ── 4. Separately tracked (excluded from base passes) ─────────────────
        crosses          = [p for p in all_passes if _has_qid(p, 2)]
        throwins         = [p for p in all_passes if _has_qid(p, 107)]
        keeper_throws    = [p for p in all_passes if _has_qid(p, 123)]
        corner_deliveries= [p for p in all_passes if _has_qid(p, 6)]   # CornerTaken

        crosses_att      = len(crosses)
        crosses_acc      = sum(1 for p in crosses if _acc(p))
        throwins_att     = len(throwins)
        throwins_acc     = sum(1 for p in throwins if _acc(p))
        kt_att           = len(keeper_throws)
        kt_acc           = sum(1 for p in keeper_throws if _acc(p))
        corners_att      = len(corner_deliveries)
        corners_acc      = sum(1 for p in corner_deliveries if _acc(p))

        # Blocked passes (separate event type 74)
        blocked_passes   = sum(1 for e in events
                               if e.get('type', {}).get('value') == 74
                               and e.get('teamId') == team_id)

        # ── 5. Chance creation (ALL pass events, no exclusion filter) ─────────

        # Key passes — KeyPass qualifier on Pass OR BallTouch events
        key_passes = sum(
            1 for e in events
            if e.get('teamId') == team_id
            and e.get('type', {}).get('value') in (1, 61)  # Pass or BallTouch
            and _has(e, 'KeyPass')
        )

        # Assists — Goal events with Assisted (QID 29), resolve via RelatedEventId
        assists = 0
        for e in events:
            if (e['type']['displayName'] == 'Goal'
                    and e.get('teamId') == team_id
                    and _has_qid(e, 29)):                   # Assisted qualifier
                rel_id = _get_qid_val(e, 55)               # RelatedEventId = QID 55
                if rel_id:
                    assist_event = event_by_id.get(int(float(rel_id)))
                    if assist_event and assist_event.get('teamId') == team_id:
                        assists += 1

        # Big chances created — ALL pass events, no exclusion filter
        # A cross or corner delivery can absolutely create a big chance
        big_chances_created = sum(
            1 for e in events
            if e.get('teamId') == team_id
            and e.get('type', {}).get('value') == 1
            and _has(e, 'BigChanceCreated')
            # No exclusion filter here — intentional
        )

        # ── 6. Spatial (final third) ──────────────────────────────────────────
        FINAL_THIRD = 66.6

        # passes_entering_final_third
        # Pass (type 1) + OffsidePass (type 2), start_x < 66.6 → end_x >= 66.6
        # Excludes KeeperThrow (123) only — crosses, throw-ins, goal kicks
        # and free kicks all count as entries (ball physically crosses the line).
        # OffsidePass included: ball entered the third regardless of offside call.
        # NOTE: will undercount vs Opta/Sofascore by ~0-2 due to absent carry data.
        passes_entering_ft = 0
        for e in all_passes + [e for e in events
                                if e.get('type',{}).get('value') == 2
                                and e.get('teamId') == team_id]:
            q_ids = {q.get('type',{}).get('value') for q in e.get('qualifiers',[])}
            if 123 in q_ids:
                continue
            end_x = _flt(_get_qid_val(e, 140))
            if end_x == 0.0:
                continue
            if float(e.get('x', 0)) < FINAL_THIRD and end_x >= FINAL_THIRD:
                passes_entering_ft += 1

        # passes_in_final_third
        # Pass (type 1) only, end_x >= 66.6, excludes {2, 107, 123}
        # (Cross, ThrowIn, KeeperThrow). Attempted and accurate counts.
        # NOTE: will undercount vs Opta/Sofascore by ~0-4 due to absent carry data.
        passes_in_ft_att = 0
        passes_in_ft_acc = 0
        for p in all_passes:
            q_ids = {q.get('type',{}).get('value') for q in p.get('qualifiers',[])}
            if q_ids & {2, 107, 123}:
                continue
            end_x = _flt(_get_qid_val(p, 140))
            if end_x == 0.0:
                continue
            if end_x >= FINAL_THIRD:
                passes_in_ft_att += 1
                if _acc(p):
                    passes_in_ft_acc += 1

        team_stats.append({
            'whoscored_match_id':       whoscored_match_id,
            'team_id':                  team_id,
            # 1. Volume (industry standard baseline)
            'passes_total':             passes_total,
            'passes_accurate':          passes_accurate,
            'passes_inaccurate':        passes_inaccurate,
            'pass_accuracy_pct':        pass_accuracy_pct,
            # 2. Pass type (base passes only)
            'longballs_attempted':      len(longballs),
            'longballs_accurate':       lb_acc,
            'longball_accuracy_pct':    round(lb_acc / len(longballs) * 100, 2) if longballs else None,
            'throughballs':             throughballs,
            'head_passes':              head_passes,
            'layoffs':                  layoffs,
            'passes_short':             passes_short,
            'passes_medium':            passes_medium,
            'passes_long_band':         passes_long_band,
            # 3. Set pieces (base passes only)
            'freekick_passes':          freekick_passes,
            'indirect_fk_passes':       indirect_fk,
            'goalkick_passes':          goalkick_passes,
            # 4. Separately tracked
            'crosses_attempted':        crosses_att,
            'crosses_accurate':         crosses_acc,
            'cross_accuracy_pct':       round(crosses_acc / crosses_att * 100, 2) if crosses_att else None,
            'corner_deliveries':        corners_att,
            'corner_deliveries_accurate': corners_acc,
            'throwins_attempted':       throwins_att,
            'throwins_accurate':        throwins_acc,
            'keeper_throws_attempted':  kt_att,
            'keeper_throws_accurate':   kt_acc,
            'blocked_passes':           blocked_passes,
            # 5. Chance creation (no exclusion filter)
            'key_passes':               key_passes,
            'assists':                  assists,
            'big_chances_created':      big_chances_created,
            # 6. Spatial (final third) — pass-based counts
            # Will undercount vs Opta/Sofascore by ~0-4 (absent carry data)
            'passes_entering_final_third':      passes_entering_ft,
            'passes_in_final_third':            passes_in_ft_att,
            'passes_in_final_third_accurate':   passes_in_ft_acc,
        })

    # ── Player-level stats ────────────────────────────────────────────────────
    player_stats = []
    names = data.get('playerIdNameDictionary', {})

    # Collect all player IDs that appear in pass events for either team
    player_ids = {
        (e.get('teamId'), e.get('playerId'))
        for e in events
        if e.get('type', {}).get('value') == 1
        and e.get('playerId')
        and e.get('teamId') in (home_id, away_id)
    }

    for team_id, player_id in sorted(player_ids):
        p_base  = [e for e in events
                   if e.get('type',{}).get('value') == 1
                   and e.get('teamId') == team_id
                   and e.get('playerId') == player_id
                   and _is_base_pass(e)]
        p_all   = [e for e in events
                   if e.get('type',{}).get('value') == 1
                   and e.get('teamId') == team_id
                   and e.get('playerId') == player_id]
        if not p_base and not p_all:
            continue

        n       = len(p_base)
        n_acc   = sum(1 for p in p_base if _acc(p))
        acc_pct = round(n_acc / n * 100, 2) if n else 0

        lb      = [p for p in p_base if _has(p, 'Longball')]
        lb_acc  = sum(1 for p in lb if _acc(p))

        # Key passes (all pass + touch events, no exclusion)
        kp = sum(1 for e in events
                 if e.get('teamId') == team_id
                 and e.get('playerId') == player_id
                 and e.get('type',{}).get('value') in (1, 61)
                 and _has(e, 'KeyPass'))

        # Assists via RelatedEventId on goals
        p_assists = 0
        for e in events:
            if (e['type']['displayName'] == 'Goal'
                    and e.get('teamId') == team_id
                    and _has_qid(e, 29)):
                rel_id = _get_qid_val(e, 55)
                if rel_id:
                    assist_evt = event_by_id.get(int(float(rel_id)))
                    if assist_evt and assist_evt.get('playerId') == player_id:
                        p_assists += 1

        # Big chances created
        bcc = sum(1 for p in p_all if _has(p, 'BigChanceCreated'))

        # Crosses
        p_crosses     = [p for p in p_all if _has_qid(p, 2)]
        p_crosses_acc = sum(1 for p in p_crosses if _acc(p))

        # Final third entries and passes (same definitions as team level)
        p_entering_ft = 0
        for e in p_all + [e for e in events
                          if e.get('type',{}).get('value') == 2
                          and e.get('teamId') == team_id
                          and e.get('playerId') == player_id]:
            q_ids = {q.get('type',{}).get('value') for q in e.get('qualifiers',[])}
            if 123 in q_ids: continue
            end_x = _flt(_get_qid_val(e, 140))
            if end_x == 0.0: continue
            if float(e.get('x', 0)) < 66.6 and end_x >= 66.6:
                p_entering_ft += 1

        p_in_ft_att = p_in_ft_acc = 0
        for p in p_all:
            q_ids = {q.get('type',{}).get('value') for q in p.get('qualifiers',[])}
            if q_ids & {2, 107, 123}: continue
            end_x = _flt(_get_qid_val(p, 140))
            if end_x == 0.0: continue
            if end_x >= 66.6:
                p_in_ft_att += 1
                if _acc(p): p_in_ft_acc += 1

        player_stats.append({
            'whoscored_match_id':             whoscored_match_id,
            'team_id':                        team_id,
            'player_id':                      player_id,
            'player_name':                    names.get(str(player_id)),
            # Volume
            'passes_total':                   n,
            'passes_accurate':                n_acc,
            'passes_inaccurate':              n - n_acc,
            'pass_accuracy_pct':              acc_pct,
            # Pass type
            'longballs_attempted':            len(lb),
            'longballs_accurate':             lb_acc,
            'longball_accuracy_pct':          round(lb_acc / len(lb) * 100, 2) if lb else None,
            'throughballs':                   sum(1 for p in p_base if _has(p, 'Throughball')),
            'head_passes':                    sum(1 for p in p_base if _has(p, 'HeadPass')),
            'layoffs':                        sum(1 for p in p_base if _has(p, 'LayOff')),
            # Crosses (separately tracked)
            'crosses_attempted':              len(p_crosses),
            'crosses_accurate':               p_crosses_acc,
            # Chance creation
            'key_passes':                     kp,
            'assists':                        p_assists,
            'big_chances_created':            bcc,
            # Spatial
            'passes_entering_final_third':    p_entering_ft,
            'passes_in_final_third':          p_in_ft_att,
            'passes_in_final_third_accurate': p_in_ft_acc,
        })

    return {'team': team_stats, 'player': player_stats}



if __name__ == "__main__":
    import json
    path = './1903468.json'
    with open(path, "r", encoding='utf-8') as file:
        file = json.load(file)

    result = parse_passing(file, 1729476)
    with open("./result.json", "w", encoding='utf-8') as fp:
        json.dump(result , fp, ensure_ascii=False)