"""
parse_game_state.py  (v2 — professional-grade game state framework)
-------------------------------------------------------------------
Derives the GAME STATE timeline from WhoScored matchCentreData, splits team
metrics by state, and logs tactical context events (goals, cards, subs,
formation changes).

DESIGN (per the match-status-phase literature and provider practice)
--------------------------------------------------------------------
Game state is a DIMENSION, not a metric. The match is segmented into
intervals at every state change; everything else joins to that spine.
Score state is stored as an explicit signed differential (professional
convention: evaluate relative to the team, winning/drawing/losing), plus
buckets for cross-season aggregation. All team-level tables are LONG
format keyed by team_id so multi-team, multi-season queries never need
home/away CASE logic.

OUTPUT TABLES
-------------
1. states          — the spine. One row per interval (cut at goals, red
                     cards, period bounds). Match-level, home/away absolute
                     scores PLUS home_score_diff (Home − Away: 0 = tied,
                     +1 = home leading by 1, −2 = home trailing by 2),
                     man_diff_home, and trigger (what started the interval:
                     kickoff / period_start / goal_home / goal_away /
                     red_card_home / red_card_away).
2. team_states     — the spine × 2, one row per interval PER TEAM with
                     score_diff / score_bucket / man_diff / man_state from
                     that team's perspective. The query workhorse:
                     WHERE team_id = X AND score_bucket = 'trailing_1'.
3. context_events  — tactical change log: every goal (with penalty /
                     own-goal flags), every card, every substitution
                     (player_out -> player_in), every formation change
                     (authoritative names from the formations array).
                     Each row is tagged with the state it occurred in and
                     the team-perspective score_diff AT that moment.
4. team_state_agg  — per (team, period, score_bucket, man_state)
                     aggregates, minutes > 0. `minutes` is the denominator
                     column: consume countable metrics per-minute-in-state.
                     PERIOD is part of the key: a row never spans the
                     half-time break, so second-half-from-kickoff and
                     first-half performance in the same score/man state stay
                     separate (teams make tactical changes at half-time).

CONVENTIONS
-----------
  - A goal event belongs to the PRE-goal state (the shot was taken under
    the old state). The new state starts immediately after.
  - Minutes: football convention (42:39 = the 43rd minute). start_minute /
    end_minute are MATCH-CLOCK based (second half starts at 46', not at
    45 + first-half stoppage), so they join cleanly to xG-provider data.
    Display strings render stoppage as "45+4'".
  - score_diff / man_diff in team rows are FROM THAT TEAM'S PERSPECTIVE.
  - Score buckets: trailing_2plus | trailing_1 | level | leading_1 |
    leading_2plus.  Man states: even | up | down.
"""

import math
from collections import defaultdict


PITCH_LENGTH    = 105.0
PITCH_WIDTH     = 68.0
FINAL_THIRD_MIN = 66.6
DEF_THIRD_MAX   = 33.4
BOX_X_MIN       = 100 - 16.5 / PITCH_LENGTH * 100
BOX_Y_MIN       = 50 - (40.32 / PITCH_WIDTH / 2) * 100
BOX_Y_MAX       = 50 + (40.32 / PITCH_WIDTH / 2) * 100

_PPDA_ACTIONS = {'Foul', 'Tackle', 'Interception', 'Challenge', 'BlockedPass'}

# ── Canonical pass definition (MUST match parse_passing.py exactly) ───────────
# A pass is event type VALUE == 1. base_passes excludes qualifier IDs:
#   2 = Cross, 107 = ThrowIn, 123 = KeeperThrow
# These are the foundation for every passing volume / accuracy number. Game
# state passing counts MUST use this so per-state sums equal the canonical
# match totals from parse_passing. (Counting by type displayName == 'Pass'
# was the bug — it swept in crosses, throw-ins and corners, inflating volume.)
PASS_TYPE_VALUE  = 1
PASS_EXCLUDE_IDS = {2, 107, 123}
# Dead-ball pass qualifiers (for open-play splits of chance creation), matching
# parse_passing_advanced.DEAD_BALL_IDS:
#   5 = FreekickTaken, 6 = CornerTaken, 107 = ThrowIn, 123 = KeeperThrow,
#   124 = GoalKick, 241 = IndirectFreekickTaken
DEAD_BALL_IDS = {5, 6, 107, 123, 124, 241}

# xT grid — Karun Singh (2018), 12 rows (x) x 8 cols (y). Identical to
# parse_passing_advanced.XT_GRID so xt_added sums match across parsers.
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


def _t(e):  return e.get('expandedMinute', 0) * 60 + (e.get('second') or 0)
def _q(e):  return {q.get('type', {}).get('displayName') for q in e.get('qualifiers', [])}
def _flt(v, d=0.0):
    try: return float(v)
    except: return d

def _type_value(e):
    return e.get('type', {}).get('value')

def _has_qid(e, qid):
    return any(q.get('type', {}).get('value') == qid for q in e.get('qualifiers', []))

def _qids(e):
    return {q.get('type', {}).get('value') for q in e.get('qualifiers', [])}

def _is_base_pass(e):
    """Canonical: pass event (type 1) not carrying Cross/ThrowIn/KeeperThrow."""
    return (_type_value(e) == PASS_TYPE_VALUE
            and not (_qids(e) & PASS_EXCLUDE_IDS))

def _is_open_play_pass_event(e):
    """No dead-ball qualifier present (matches parse_passing_advanced)."""
    return not (_qids(e) & DEAD_BALL_IDS)

def _xt(x, y):
    col = min(int(_flt(y) / 100 * 8), 7)
    row = min(int(_flt(x) / 100 * 12), 11)
    if row < 0 or col < 0:
        return 0.0
    return XT_GRID[row][col]

def _qid_val(e, qid):
    for q in e.get('qualifiers', []):
        if q.get('type', {}).get('value') == qid:
            return q.get('value')
    return None


def score_bucket(diff: int) -> str:
    if diff <= -2: return 'trailing_2plus'
    if diff == -1: return 'trailing_1'
    if diff ==  0: return 'level'
    if diff ==  1: return 'leading_1'
    return 'leading_2plus'

def man_state(diff: int) -> str:
    return 'even' if diff == 0 else ('up' if diff > 0 else 'down')


# ── Timeline ──────────────────────────────────────────────────────────────────

class GameStateTimeline:
    """Builds the state-interval spine; tags arbitrary times with state."""

    def __init__(self, data: dict):
        self.home_id = data['home']['teamId']
        self.away_id = data['away']['teamId']
        events = sorted(data.get('events', []),
                        key=lambda e: (_t(e), e.get('eventId', 0)))

        # Period bounds keyed by period VALUE (1=1st half, 2=2nd half,
        # 3/4=extra time). Start/End events are duplicated per team, and
        # WhoScored emits a bogus PostGame (value 14) End event with a
        # nonsense expandedMinute — ignore anything above ET periods and
        # take min(Start)/max(End) per period value.
        p_start, p_end = {}, {}
        self._clock_shift = {}
        for e in events:
            etype = e['type']['displayName']
            if etype not in ('Start', 'End'):
                continue
            pv = e.get('period', {}).get('value', 0)
            if not 1 <= pv <= 4:
                continue
            tm = _t(e)
            if etype == 'Start':
                p_start[pv] = min(p_start.get(pv, tm), tm)
                # Raw match-clock at this Start (WhoScored provides it directly:
                # P2 Start is always minute=45, second=0). The internal clock
                # `tm` is expandedMinute-based; the offset between them is the
                # cumulative stoppage carried into this period.
                clock = (e.get('minute', 0) or 0) * 60 + (e.get('second') or 0)
                self._clock_shift[pv] = min(
                    self._clock_shift.get(pv, tm - clock), tm - clock)
            else:
                p_end[pv] = max(p_end.get(pv, tm), tm)
        period_bounds = [(p_start[pv], p_end[pv], pv)
                         for pv in sorted(p_start) if pv in p_end]
        self._nominal_end = {1: 45, 2: 90, 3: 105, 4: 120}

        # State-change moments
        changes = []   # (time, eventId, kind, credited_team)
        for e in events:
            etype = e['type']['displayName']
            tm = _t(e)
            if etype == 'Goal':
                own = 'OwnGoal' in _q(e)
                scorer_team = e.get('teamId')
                credited = (self.away_id if scorer_team == self.home_id
                            else self.home_id) if own else scorer_team
                changes.append((tm, e.get('eventId', 0), 'goal', credited))
            elif etype == 'Card':
                if _q(e) & {'Red', 'SecondYellow'}:
                    changes.append((tm, e.get('eventId', 0), 'red',
                                    e.get('teamId')))

        if not period_bounds:
            period_bounds = [(0.0, max((_t(e) for e in events),
                                       default=5400.0), 1)]
        self.period_bounds = period_bounds
        self.match_end = period_bounds[-1][1]

        changes.sort(key=lambda c: (c[0], c[1]))
        intervals = []
        hs = as_ = 0
        hp = ap = 11
        ci = 0
        sid = 0
        for p_start_s, p_end_s, period in period_bounds:
            cursor = p_start_s
            trigger = 'kickoff' if period == 1 else 'period_start'
            while True:
                nxt = changes[ci] if ci < len(changes) else None
                if nxt is not None and nxt[0] <= p_end_s:
                    cut = max(nxt[0], cursor)
                    if cut > cursor:
                        intervals.append(dict(
                            state_id=sid, period=period,
                            start_s=cursor, end_s=cut,
                            duration_s=round(cut - cursor, 1),
                            home_score=hs, away_score=as_,
                            home_players=hp, away_players=ap,
                            trigger=trigger))
                        sid += 1
                    if nxt[2] == 'goal':
                        if nxt[3] == self.home_id: hs += 1
                        else: as_ += 1
                        trigger = ('goal_home' if nxt[3] == self.home_id
                                   else 'goal_away')
                    else:
                        if nxt[3] == self.home_id: hp -= 1
                        else: ap -= 1
                        trigger = ('red_card_home' if nxt[3] == self.home_id
                                   else 'red_card_away')
                    cursor = cut
                    ci += 1
                else:
                    if p_end_s > cursor:
                        intervals.append(dict(
                            state_id=sid, period=period,
                            start_s=cursor, end_s=p_end_s,
                            duration_s=round(p_end_s - cursor, 1),
                            home_score=hs, away_score=as_,
                            home_players=hp, away_players=ap,
                            trigger=trigger))
                        sid += 1
                    break
        self.intervals = intervals

    # ── clock conversion ──────────────────────────────────────────────────
    def clock_seconds(self, expanded_s: float, period: int) -> float:
        """Internal expanded-seconds -> true match-clock seconds, using the
        per-period offset anchored on WhoScored's raw minute/second clock."""
        shift = self._clock_shift.get(period, 0.0)
        return max(0.0, expanded_s - shift)

    def clock_minute(self, expanded_s: float, period: int):
        """Internal expanded-seconds -> (minute, second, display).

        `minute`/`second` are the RAW match clock exactly as WhoScored (and
        parse_shooting) report them: Igor Jesus' 44:41 goal -> minute=44,
        second=41. This is the join key to xG/shooting data — those parsers
        store the raw clock verbatim, so game-state rows must too.

        `display` is the human football-convention label where 44:41 reads as
        the 45th minute and stoppage renders "45+3'" / "90+6'".
        """
        clock_s = self.clock_seconds(expanded_s, period)
        minute = int(clock_s // 60)
        second = int(round(clock_s - minute * 60))
        if second >= 60:                      # rounding spill
            minute += 1
            second -= 60
        # Football-convention display: 44:41 -> 45th minute.
        disp_minute = minute + 1 if second > 0 else (minute if minute > 0 else 1)
        # Simpler & matches stated convention (42:39 = 43'): always ceil.
        disp_minute = minute + 1 if second > 0 else max(minute, 1)
        nominal = self._nominal_end.get(period, 45 * period)
        disp = (f"{nominal}+{disp_minute - nominal}'" if disp_minute > nominal
                else f"{disp_minute}'")
        return minute, second, disp

    # ── lookup ────────────────────────────────────────────────────────────
    def state_at(self, t_seconds: float):
        """Interval containing t. A goal event's own timestamp resolves to
        the PRE-goal interval (end-inclusive)."""
        for iv in self.intervals:
            if iv['start_s'] < t_seconds <= iv['end_s']:
                return iv
        # t falls in a dead zone (exactly at a period start, before kickoff,
        # or in the half-time gap): resolve to the next interval that starts
        # at/after t — e.g. a 46' half-time substitution belongs to the
        # second-half opening state, not the final state of the match.
        following = [iv for iv in self.intervals if iv['start_s'] >= t_seconds]
        if following:
            return min(following, key=lambda iv: iv['start_s'])
        return self.intervals[-1] if self.intervals else None

    def tag(self, t_seconds: float, team_id: int) -> dict:
        """Game-state tag from `team_id`'s perspective at time t."""
        iv = self.state_at(t_seconds)
        if iv is None:
            return {'state_id': None, 'period': None, 'score_diff': None,
                    'score_bucket': None, 'man_diff': None, 'man_state': None}
        home = (team_id == self.home_id)
        sd = (iv['home_score'] - iv['away_score']) if home \
             else (iv['away_score'] - iv['home_score'])
        md = (iv['home_players'] - iv['away_players']) if home \
             else (iv['away_players'] - iv['home_players'])
        return {'state_id': iv['state_id'], 'period': iv['period'],
                'score_diff': sd, 'score_bucket': score_bucket(sd),
                'man_diff': md, 'man_state': man_state(md)}


# ── Context events (tactical change log) ─────────────────────────────────────

def _build_context_events(data, tl, whoscored_match_id):
    home_id, away_id = tl.home_id, tl.away_id
    names = data.get('playerIdNameDictionary', {})
    events = sorted(data.get('events', []),
                    key=lambda e: (_t(e), e.get('eventId', 0)))
    rows = []

    def base_row(tm, team_id, period):
        tag = tl.tag(tm, team_id)
        minute, second, disp = tl.clock_minute(tm, period)
        return {
            'whoscored_match_id': whoscored_match_id,
            'team_id':            team_id,
            'state_id':           tag['state_id'],
            'score_diff_at':      tag['score_diff'],
            'man_state_at':       tag['man_state'],
            'period':             period,
            'minute':             minute,
            'second':             second,
            'minute_display':     disp,
            'time_s':             round(tm, 1),
        }

    for e in events:
        etype = e['type']['displayName']
        tm = _t(e)
        period = e.get('period', {}).get('value', 1)
        team = e.get('teamId')
        if team not in (home_id, away_id):
            continue
        q = _q(e)
        pid = e.get('playerId')

        if etype == 'Goal':
            own = 'OwnGoal' in q
            credited = (away_id if team == home_id else home_id) if own else team
            r = base_row(tm, credited, period)
            r.update(event_type=('own_goal' if own else
                                 'penalty_goal' if 'Penalty' in q else 'goal'),
                     player_id=pid, player_name=names.get(str(pid)),
                     player_in_id=None, player_in_name=None,
                     player_out_id=None, player_out_name=None, detail=None)
            rows.append(r)
        elif etype == 'Card':
            kind = ('red_card' if 'Red' in q else
                    'second_yellow' if 'SecondYellow' in q else 'yellow_card')
            r = base_row(tm, team, period)
            r.update(event_type=kind,
                     player_id=pid, player_name=names.get(str(pid)),
                     player_in_id=None, player_in_name=None,
                     player_out_id=None, player_out_name=None, detail=None)
            rows.append(r)
        elif etype == 'SubstitutionOn':
            off_id = e.get('relatedPlayerId')
            r = base_row(tm, team, period)
            r.update(event_type='substitution',
                     player_id=None, player_name=None,
                     player_in_id=pid, player_in_name=names.get(str(pid)),
                     player_out_id=off_id,
                     player_out_name=names.get(str(off_id)),
                     detail=None)
            rows.append(r)

    # Formation changes — authoritative names from the formations array.
    for side, tid in (('home', home_id), ('away', away_id)):
        forms = data.get(side, {}).get('formations', [])
        prev_name = None
        for f in forms:
            name = f.get('formationName')
            start_min = f.get('startMinuteExpanded', 0)
            if prev_name is None:
                prev_name = name
                continue                       # starting formation, not a change
            if name != prev_name:
                tm = start_min * 60 + 1e-6
                period = next((pv for (ps, pe, pv) in tl.period_bounds
                               if ps <= tm <= pe), 1)
                r = base_row(tm, tid, period)
                r.update(event_type='formation_change',
                         player_id=None, player_name=None,
                         player_in_id=None, player_in_name=None,
                         player_out_id=None, player_out_name=None,
                         detail=f"{prev_name}->{name}")
                rows.append(r)
                prev_name = name

    rows.sort(key=lambda r: r['time_s'])
    return rows


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_game_state(data: dict, whoscored_match_id: int) -> dict:
    """
    Returns:
        {
          'states':         [interval spine rows — match level],
          'team_states':    [spine x2, one row per interval per team],
          'context_events': [goals / cards / subs / formation changes],
          'team_state_agg': [per (team, score_bucket, man_state), minutes>0]
        }
    """
    tl = GameStateTimeline(data)
    home_id, away_id = tl.home_id, tl.away_id
    events = sorted(data.get('events', []),
                    key=lambda e: (_t(e), e.get('eventId', 0)))

    # ── 1. spine ──────────────────────────────────────────────────────────
    spine = []
    for iv in tl.intervals:
        sm, ss, sd_disp = tl.clock_minute(iv['start_s'] + 1e-6, iv['period'])
        em, es, ed_disp = tl.clock_minute(iv['end_s'], iv['period'])
        spine.append(dict(
            whoscored_match_id=whoscored_match_id, **iv,
            home_score_diff=iv['home_score'] - iv['away_score'],
            man_diff_home=iv['home_players'] - iv['away_players'],
            start_minute=sm, start_second=ss, start_display=sd_disp,
            end_minute=em,  end_second=es,  end_display=ed_disp))

    # ── 2. team_states (per-interval per-team perspective) ────────────────
    team_states = []
    for s in spine:
        for tid in (home_id, away_id):
            home = (tid == home_id)
            sd = s['home_score_diff'] if home else -s['home_score_diff']
            md = s['man_diff_home'] if home else -s['man_diff_home']
            team_states.append({
                'whoscored_match_id': whoscored_match_id,
                'state_id':      s['state_id'],
                'team_id':       tid,
                'opponent_id':   away_id if home else home_id,
                'is_home':       home,
                'period':        s['period'],
                'start_s':       s['start_s'],
                'end_s':         s['end_s'],
                'duration_s':    s['duration_s'],
                'start_minute':  s['start_minute'],
                'end_minute':    s['end_minute'],
                'start_display': s['start_display'],
                'end_display':   s['end_display'],
                'score_for':     s['home_score'] if home else s['away_score'],
                'score_against': s['away_score'] if home else s['home_score'],
                'score_diff':    sd,
                'score_bucket':  score_bucket(sd),
                'man_diff':      md,
                'man_state':     man_state(md),
                'trigger':       s['trigger'],
            })

    # ── 3. context events ─────────────────────────────────────────────────
    context_events = _build_context_events(data, tl, whoscored_match_id)

    # ── 4. aggregates ─────────────────────────────────────────────────────
    agg = defaultdict(lambda: defaultdict(float))
    state_ids_of = defaultdict(list)

    for iv in tl.intervals:
        for tid in (home_id, away_id):
            tag = tl.tag((iv['start_s'] + iv['end_s']) / 2, tid)
            key = (tid, tag['score_bucket'], tag['man_state'], iv['period'])
            agg[key]['minutes'] += iv['duration_s'] / 60.0
            state_ids_of[key].append(iv['state_id'])

    # possession from WhoScored per-minute possession values
    poss = {home_id: data['home']['stats'].get('possession', {}),
            away_id: data['away']['stats'].get('possession', {})}
    for mkey in set(poss[home_id]) | set(poss[away_id]):
        try:
            t_mid = (int(mkey) + 0.5) * 60
        except ValueError:
            continue
        hv = _flt(poss[home_id].get(mkey, 0))
        av = _flt(poss[away_id].get(mkey, 0))
        if hv + av <= 0:
            continue
        for tid, v in ((home_id, hv), (away_id, av)):
            tag = tl.tag(t_mid, tid)
            key = (tid, tag['score_bucket'], tag['man_state'], tag['period'])
            agg[key]['poss_own'] += v
            agg[key]['poss_all'] += hv + av

    # Pre-resolve assist events: a Goal with the Assisted qualifier (QID 29)
    # points via RelatedEventId (QID 55) to the assisting pass/touch. We tag
    # the ASSIST at the assisting event's time/state (consistent with where
    # the creative action happened). Matches parse_passing assist logic.
    event_by_id = {e.get('eventId'): e for e in events}
    assist_event_ids = set()
    for e in events:
        if (e['type']['displayName'] == 'Goal'
                and e.get('teamId') in (home_id, away_id)
                and _has_qid(e, 29)):
            rel = _qid_val(e, 55)
            if rel is not None:
                ae = event_by_id.get(int(float(rel)))
                if ae and ae.get('teamId') == e.get('teamId'):
                    assist_event_ids.add(ae.get('eventId'))

    for e in events:
        tid = e.get('teamId')
        if tid not in (home_id, away_id):
            continue
        etype = e['type']['displayName']
        tval = _type_value(e)
        outcome = e.get('outcomeType', {}).get('displayName')
        acc = outcome == 'Successful'
        tm = _t(e)
        x, y = _flt(e.get('x'), -1), _flt(e.get('y'), -1)
        q = _q(e)
        qids = _qids(e)
        tag = tl.tag(tm, tid)
        key = (tid, tag['score_bucket'], tag['man_state'], tag['period'])
        other = away_id if tid == home_id else home_id
        otag = tl.tag(tm, other)
        okey = (other, otag['score_bucket'], otag['man_state'], otag['period'])
        a = agg[key]

        if e.get('isTouch'):
            a['touches'] += 1
            if x > FINAL_THIRD_MIN:
                a['ft_touches'] += 1
                agg[okey]['ft_touches_opp'] += 1
            if x >= BOX_X_MIN and BOX_Y_MIN <= y <= BOX_Y_MAX:
                a['box_touches'] += 1
        if e.get('isShot'):
            a['shots'] += 1
            if (etype == 'Goal'
                    or (etype == 'SavedShot' and 'Blocked' not in q)):
                a['shots_on_target'] += 1
            if etype == 'Goal':
                a['goals'] += 1
            # big_chances: shot-side metric (BigChance qualifier on the shot),
            # distinct from big_chances_created. A team can face/have more big
            # chances than it creates. Definition mirrors parse_shooting.
            if 'BigChance' in q:
                a['big_chances'] += 1
                if etype == 'Goal':
                    a['big_chances_scored'] += 1

        # ── Passing: CANONICAL definition (type value 1, base-pass filter) ──
        if tval == PASS_TYPE_VALUE:
            is_base = not (qids & PASS_EXCLUDE_IDS)
            if is_base:
                a['passes'] += 1
                if acc:
                    a['passes_completed'] += 1
                # xT added — ALL base passes (no accuracy gate), identical to
                # parse_passing_advanced which sums xt_added over every base
                # pass row. End coords: QID 140/141 == PassEndX/Y == endX/Y.
                end_x = _flt(_qid_val(e, 140) or e.get('endX') or x)
                end_y = _flt(_qid_val(e, 141) or e.get('endY') or y)
                xt_add = _xt(end_x, end_y) - _xt(x, y)
                a['xt_total'] += xt_add
                if xt_add > 0:
                    a['xt_positive'] += xt_add
            # Crosses / corners tracked separately (NOT in base passes),
            # matching parse_passing's separate tracking.
            if 2 in qids:
                a['crosses'] += 1
            if 6 in qids:
                a['corners'] += 1
            # PPDA: opposition pass faced outside the presser's defensive third.
            if x <= FINAL_THIRD_MIN:
                agg[okey]['ppda_passes_faced'] += 1

        # ── Chance creation (KeyPass on Pass OR BallTouch; no base filter) ──
        if tval in (1, 61) and 'KeyPass' in q:
            if e.get('eventId') not in assist_event_ids:   # Opta: KP excludes assists
                a['key_passes'] += 1
                if _is_open_play_pass_event(e):
                    a['key_passes_open_play'] += 1
        # big_chances_created: BigChanceCreated qualifier on Pass events only
        if tval == 1 and 'BigChanceCreated' in q:
            a['big_chances_created'] += 1
            if _is_open_play_pass_event(e):
                a['big_chances_created_open_play'] += 1
        # Assists: tagged at the assisting event
        if e.get('eventId') in assist_event_ids:
            a['assists'] += 1

        if etype in _PPDA_ACTIONS and x >= DEF_THIRD_MAX:
            a['def_actions'] += 1
        if etype == 'Foul':
            if acc: a['fouls_won'] += 1
            else:   a['fouls_committed'] += 1
        if etype == 'OffsideGiven':
            a['offsides'] += 1
        if etype == 'Card':
            if 'Yellow' in q and 'SecondYellow' not in q:
                a['yellow_cards'] += 1
            if q & {'Red', 'SecondYellow'}:
                a['red_cards'] += 1
        if etype == 'SubstitutionOn':
            a['subs_made'] += 1
        if etype == 'TakeOn':
            a['take_ons'] += 1
            if acc: a['take_ons_won'] += 1
        if etype == 'Tackle':        a['tackles'] += 1
        if etype == 'Interception':  a['interceptions'] += 1
        if etype == 'Clearance':     a['clearances'] += 1
        if etype == 'Aerial':
            a['aerials_total'] += 1
            if acc: a['aerials_won'] += 1

    team_state_agg = []
    for (tid, sb, ms, period), a in sorted(
            agg.items(),
            key=lambda kv: (kv[0][0], kv[0][3], kv[0][1], kv[0][2])):
        if a['minutes'] <= 0:
            continue
        ft_all = a['ft_touches'] + a['ft_touches_opp']
        team_state_agg.append({
            'whoscored_match_id':  whoscored_match_id,
            'team_id':             tid,
            'period':              period,
            'score_bucket':        sb,
            'man_state':           ms,
            'state_ids':           state_ids_of.get((tid, sb, ms, period), []),
            'minutes':             round(a['minutes'], 1),
            'possession_pct':      round(a['poss_own'] / a['poss_all'] * 100, 1)
                                   if a['poss_all'] else None,
            'touches':             int(a['touches']),
            'final_third_touches': int(a['ft_touches']),
            'field_tilt_pct':      round(a['ft_touches'] / ft_all * 100, 1)
                                   if ft_all else None,
            'box_touches':         int(a['box_touches']),
            'shots':               int(a['shots']),
            'shots_on_target':     int(a['shots_on_target']),
            'goals':               int(a['goals']),
            # ── Chance creation (canonical defs, match parse_passing/_advanced) ──
            'key_passes':                    int(a['key_passes']),
            'key_passes_open_play':          int(a['key_passes_open_play']),
            'assists':                       int(a['assists']),
            'big_chances_created':           int(a['big_chances_created']),
            'big_chances_created_open_play': int(a['big_chances_created_open_play']),
            # big_chances = shot-side (BigChance qualifier on shots), NOT the
            # same as big_chances_created. A team can have more big chances
            # than it creates (e.g. created off a carry/rebound, not a pass).
            'big_chances':                   int(a['big_chances']),
            'big_chances_scored':            int(a['big_chances_scored']),
            # xT (base passes only; identical grid to parse_passing_advanced)
            'xt_total':            round(a['xt_total'], 4),
            'xt_positive':         round(a['xt_positive'], 4),
            'passes':              int(a['passes']),
            'passes_completed':    int(a['passes_completed']),
            'pass_accuracy_pct':   round(a['passes_completed'] / a['passes'] * 100, 1)
                                   if a['passes'] else None,
            'crosses':             int(a['crosses']),
            'corners':             int(a['corners']),
            'fouls_committed':     int(a['fouls_committed']),
            'fouls_won':           int(a['fouls_won']),
            'offsides':            int(a['offsides']),
            'yellow_cards':        int(a['yellow_cards']),
            'red_cards':           int(a['red_cards']),
            'subs_made':           int(a['subs_made']),
            'take_ons':            int(a['take_ons']),
            'take_ons_won':        int(a['take_ons_won']),
            'tackles':             int(a['tackles']),
            'interceptions':       int(a['interceptions']),
            'clearances':          int(a['clearances']),
            'aerials_won':         int(a['aerials_won']),
            'aerials_total':       int(a['aerials_total']),
            'defensive_actions':   int(a['def_actions']),
            'ppda':                round(a['ppda_passes_faced'] / a['def_actions'], 2)
                                   if a['def_actions'] else None,
        })

    return {
        'states':         spine,
        'team_states':    team_states,
        'context_events': context_events,
        'team_state_agg': team_state_agg,
    }


if __name__ == "__main__":
    import json
    path = './1903468.json'
    with open(path, "r", encoding='utf-8') as fp:
        match_data = json.load(fp)
    result = parse_game_state(match_data, 1729476)
    with open("./result_game_state.json", "w", encoding='utf-8') as fp:
        json.dump(result, fp, indent=4)