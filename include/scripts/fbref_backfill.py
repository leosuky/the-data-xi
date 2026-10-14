"""
include/scripts/fbref_backfill.py
=================================
Manual backfill of WhoScored RAW tables from an FBref multi-sheet xlsx export,
for the rare match whose WhoScored matchCentreData is missing/garbage.

WHEN TO RUN
-----------
After the normal load runs and a match comes up WhoScored-absent (the loader
logs "WhoScored matchCentreData missing/invalid — loading other providers
only"). At that point Fotmob/Sofascore/odds are already loaded for the match;
this script patches in the WhoScored-shaped *summary* tables from FBref so the
per-player and shot facts light up too.

WHAT IT DOES / DOESN'T DO
-------------------------
FBref ships aggregate stat lines, not the WhoScored event stream. So this
backfills the SUMMARY tables (ws_player_shooting/passing/possession/carries/
defending/discipline/goalkeep, ws_lineups) and ws_shots (per shot, incl. SCA
and PSxG→xgot). It does NOT synthesize the event-derived artefacts (xT, PPDA,
possession sequences, shot x/y, game-state buckets) — those stay empty.

Crucially it writes ws_match_meta with has_ws_events=FALSE — so the match has
WhoScored-side metadata (team ids, which let dim_match resolve home/away
team_uid) while still reporting no event data. dim_match reads has_ws_events
from that column: true for genuine event-stream loads, false here. The
backfilled summary rows attach to the match via combo_id and resolve identity
through the players' ws_player_ids, which are already known from their other
matches.

IDENTITY
--------
FBref rows key on player NAME (+ shirt #) and squad name. WhoScored RAW tables
key on ws_player_id / ws_team_id. The resolver maps names → ids using the
EXISTING ws_lineups of the two teams (every player/team appears in their other
matches). Unresolved players are logged and skipped — never guessed.

IDEMPOTENT
----------
Every target table is delete-by-combo_id then insert, so re-running is safe.
Pass dry_run=True to map + report without writing.
"""

import os
import re
import math
import logging
import unicodedata

import pandas as pd

log = logging.getLogger(__name__)

YARD_M = 0.9144  # FBref distances are in yards; WhoScored *_m columns are metres.

# Sheets are <Home|Away>_Team_<Category>; Shot_Data is shared.
CATEGORIES = ['Summary', 'Passing', 'Pass_Types', 'Possession',
              'Defense', 'Miscellaneous', 'Keeper']


# ──────────────────────────────────────────────────────────────────────────
#  value cleaning
# ──────────────────────────────────────────────────────────────────────────
def _clean(v):
    """FBref blanks come through as '·', '', or NaN — normalise to None."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        if s in ('', '·', '-', '—'):
            return None
        return s
    return v


def _to_int(v):
    v = _clean(v)
    if v is None:
        return None
    try:
        return int(round(float(v)))
    except (ValueError, TypeError):
        return None


def _to_num(v):
    v = _clean(v)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _yd_to_m(v):
    n = _to_num(v)
    return round(n * YARD_M, 2) if n is not None else None


def _parse_age(v):
    """'24-183' (years-days) → 24."""
    v = _clean(v)
    if v is None:
        return None
    return _to_int(str(v).split('-')[0])


def _parse_minute(v):
    """'45+2' → (45, 47); '67' → (67, 67). Returns (base, expanded)."""
    v = _clean(v)
    if v is None:
        return None, None
    m = re.match(r'^(\d+)(?:\+(\d+))?$', str(v).strip())
    if not m:
        return _to_int(v), _to_int(v)
    base = int(m.group(1))
    added = int(m.group(2)) if m.group(2) else 0
    return base, base + added


def normalize_name(name):
    """
    Mirror of the dbt normalize_name macro: strip accents, lowercase, drop
    punctuation, collapse whitespace. Used to match FBref names to ws_lineups.
    """
    s = _clean(name)
    if s is None:
        return None
    s = unicodedata.normalize('NFKD', str(s))
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r'\([^)]*\)', ' ', s)          # drop FBref tags like '(pen)' '(og)'
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s or None


# ──────────────────────────────────────────────────────────────────────────
#  read the workbook
# ──────────────────────────────────────────────────────────────────────────
def _player_name(v):
    """Display name with FBref's trailing '(pen)'/'(og)' tag removed."""
    s = _clean(v)
    if s is None:
        return None
    return re.sub(r'\s*\((?:pen|og)\)\s*$', '', s, flags=re.IGNORECASE).strip() or None


def _clean_frame(df):
    """Drop fully-empty rows and FBref's blank separator row in shot tables
    (rows with no Player). Columns are kept verbatim (Category_Metric)."""
    df = df.dropna(how='all')
    if 'Player' in df.columns:
        df = df[df['Player'].notna() & (df['Player'].astype(str).str.strip() != '')]
    return df.reset_index(drop=True)


def read_sheets(path):
    """
    Return {sheet_name: DataFrame} from either:
      - a directory of CSVs   → one file per sheet, filename stem == sheet name
        (e.g. Home_Team_Summary.csv, Shot_Data.csv), or
      - a single .xlsx        → one sheet per tab.
    Either way the sheet keys match the old workbook tab names, so the rest of
    the pipeline is unchanged.
    """
    sheets = {}
    if os.path.isdir(path):
        for fname in sorted(os.listdir(path)):
            if not fname.lower().endswith('.csv'):
                continue
            sn = os.path.splitext(fname)[0]
            sheets[sn] = _clean_frame(pd.read_csv(os.path.join(path, fname)))
    else:
        xl = pd.ExcelFile(path)
        for sn in xl.sheet_names:
            sheets[sn] = _clean_frame(xl.parse(sn))
    return sheets


# back-compat alias
read_workbook = read_sheets


def _team_frames(sheets, side):
    """All seven category frames for one side, keyed by category."""
    out = {}
    for cat in CATEGORIES:
        key = f'{side}_Team_{cat}'
        if key in sheets:
            out[cat] = sheets[key]
    return out


# ──────────────────────────────────────────────────────────────────────────
#  identity resolution
# ──────────────────────────────────────────────────────────────────────────
class Identity:
    """
    Resolves FBref player/squad names → ws_player_id / ws_team_id.

    Built from existing ws_lineups rows for the two squads (their other
    matches). Inject `roster` directly for testing; otherwise call
    `from_db(conn, schema, squads)`.

    roster: list of dicts {player_id, player_name, team_id, team_name}
    """

    def __init__(self, roster):
        self._team_by_name = {}                 # norm team name -> team_id
        self._player_by_team = {}               # team_id -> {norm name -> player_id}
        for r in roster:
            tn = normalize_name(r.get('team_name'))
            if tn and r.get('team_id') is not None:
                self._team_by_name.setdefault(tn, r['team_id'])
            tid = r.get('team_id')
            pn = normalize_name(r.get('player_name'))
            if tid is not None and pn:
                self._player_by_team.setdefault(tid, {}).setdefault(pn, r['player_id'])
        self.unresolved_players = []
        self.unresolved_teams = []

    @classmethod
    def from_db(cls, conn, schema, squads):
        cur = conn.cursor()
        cur.execute(
            f"""SELECT DISTINCT player_id, player_name, team_id, team_name
                FROM {schema}.ws_lineups
                WHERE team_name IS NOT NULL""")
        roster = [{'player_id': p, 'player_name': pn, 'team_id': t, 'team_name': tn}
                  for (p, pn, t, tn) in cur.fetchall()]
        cur.close()
        return cls(roster)

    def team_id(self, squad_name):
        tn = normalize_name(squad_name)
        tid = self._team_by_name.get(tn)
        if tid is None:
            # fall back to a loose contains-match (e.g. 'Norwich' vs 'Norwich City')
            for name, cand in self._team_by_name.items():
                if tn and (tn in name or name in tn):
                    return cand
            if squad_name not in self.unresolved_teams:
                self.unresolved_teams.append(squad_name)
        return tid

    def player_id(self, player_name, team_id):
        pn = normalize_name(player_name)
        pid = self._player_by_team.get(team_id, {}).get(pn)
        if pid is None and pn:
            # loose: surname match within the team
            surname = pn.split(' ')[-1]
            for name, cand in self._player_by_team.get(team_id, {}).items():
                if name.split(' ')[-1] == surname:
                    pid = cand
                    break
        if pid is None:
            self.unresolved_players.append((player_name, team_id))
        return pid


# ──────────────────────────────────────────────────────────────────────────
#  per-table row builders  (FBref column  ->  WhoScored column)
# ──────────────────────────────────────────────────────────────────────────
def _base(row, combo_id, ws_match_id, team_id, ident):
    """Common key columns + resolved player_id for a player-summary row."""
    pid = ident.player_id(row['Player'], team_id)
    return {
        'combo_id': combo_id,
        'whoscored_match_id': ws_match_id,
        'team_id': team_id,
        'player_id': pid,
        'player_name': _player_name(row['Player']),
    }, pid


def build_match_meta(combo_id, ws_match_id, home, away):
    """
    One ws_match_meta row so the match has WhoScored-side metadata (team ids
    → home/away team_uid resolve in dim_match) WITHOUT claiming event data:
    has_ws_events is explicitly False. Score/venue/referee/etc. are left to the
    DB as NULL — FBref doesn't carry them reliably, and dim_match sources those
    from Fotmob/Sofascore anyway.
    """
    return [{
        'whoscored_match_id': ws_match_id,
        'combo_id':           combo_id,
        'home_team':          home['team_name'],
        'home_team_id':       home['team_id'],
        'away_team':          away['team_name'],
        'away_team_id':       away['team_id'],
        'has_ws_events':      False,
    }]


def build_lineups(frames, combo_id, ws_match_id, team_id, team_name, is_home, ident):
    rows = []
    df = frames.get('Summary')
    if df is None:
        return rows
    for _, r in df.iterrows():
        pid = ident.player_id(r['Player'], team_id)
        if pid is None:
            continue
        rows.append({
            'whoscored_match_id': ws_match_id,
            'combo_id': combo_id,
            'player_id': pid,
            'player_name': _player_name(r['Player']),
            'team_id': team_id,
            'team_name': team_name,
            'is_home_team': is_home,
            'shirt_number': _to_int(r.get('#')),
            'position': _clean(r.get('Pos')),
            'is_starter': None,          # FBref doesn't flag starters
            'is_captain': None,
            'is_man_of_the_match': None,
            'age': _parse_age(r.get('Age')),
            'height_cm': None, 'weight_kg': None,
            'subbed_in_minute': None, 'subbed_in_for': None,
            'subbed_out_minute': None, 'subbed_out_for': None,
        })
    return rows


def build_shooting(frames, combo_id, ws_match_id, team_id, ident):
    rows = []
    df = frames.get('Summary')
    if df is None:
        return rows
    for _, r in df.iterrows():
        base, pid = _base(r, combo_id, ws_match_id, team_id, ident)
        if pid is None:
            continue
        base.update({
            'total_shots':     _to_int(r.get('Performance_Sh')),
            'goals':           _to_int(r.get('Performance_Gls')),
            'shots_on_target': _to_int(r.get('Performance_SoT')),
            'sca':             _to_int(r.get('SCA_SCA')),
        })
        rows.append(base)
    return rows


def build_passing(frames, combo_id, ws_match_id, team_id, ident):
    rows = []
    df = frames.get('Passing')
    pt = frames.get('Pass_Types')
    pt_by_player = {}
    if pt is not None:
        for _, r in pt.iterrows():
            pt_by_player[normalize_name(r['Player'])] = r
    if df is None:
        return rows
    for _, r in df.iterrows():
        base, pid = _base(r, combo_id, ws_match_id, team_id, ident)
        if pid is None:
            continue
        att, cmp_ = _to_int(r.get('Total_Att')), _to_int(r.get('Total_Cmp'))
        ptr = pt_by_player.get(normalize_name(r['Player']))
        base.update({
            'passes_total':       att,
            'passes_accurate':    cmp_,
            'passes_inaccurate':  (att - cmp_) if (att is not None and cmp_ is not None) else None,
            'pass_accuracy_pct':  _to_num(r.get('Total_Cmp%')),
            'longballs_attempted': _to_int(r.get('Long_Att')),
            'longballs_accurate':  _to_int(r.get('Long_Cmp')),
            'longball_accuracy_pct': _to_num(r.get('Long_Cmp%')),
            'throughballs':       _to_int(ptr.get('Pass Types_TB')) if ptr is not None else None,
            'crosses_attempted':  _to_int(ptr.get('Pass Types_Crs')) if ptr is not None else None,
            'key_passes':         _to_int(r.get('KP')),
            'assists':            _to_int(r.get('Ast')),
            'passes_entering_final_third': _to_int(r.get('1/3')),
        })
        rows.append(base)
    return rows


def build_passing_adv(frames, combo_id, ws_match_id, team_id, ident):
    rows = []
    df = frames.get('Passing')
    if df is None:
        return rows
    for _, r in df.iterrows():
        base, pid = _base(r, combo_id, ws_match_id, team_id, ident)
        if pid is None:
            continue
        base.update({
            'progressive_passes':          _to_int(r.get('PrgP')),
            'passes_entering_final_third': _to_int(r.get('1/3')),
            'penalty_area_passes':         _to_int(r.get('PPA')),
            'key_passes':                  _to_int(r.get('KP')),
            'assists':                     _to_int(r.get('Ast')),
        })
        rows.append(base)
    return rows


def build_possession(frames, combo_id, ws_match_id, team_id, ident):
    rows = []
    df = frames.get('Possession')
    if df is None:
        return rows
    for _, r in df.iterrows():
        base, pid = _base(r, combo_id, ws_match_id, team_id, ident)
        if pid is None:
            continue
        att, succ = _to_int(r.get('Take-Ons_Att')), _to_int(r.get('Take-Ons_Succ'))
        base.update({
            'touches':              _to_int(r.get('Touches_Touches')),
            'touches_own_third':    _to_int(r.get('Touches_Def 3rd')),
            'touches_middle_third': _to_int(r.get('Touches_Mid 3rd')),
            'touches_final_third':  _to_int(r.get('Touches_Att 3rd')),
            'touches_in_box':       _to_int(r.get('Touches_Att Pen')),
            'dribbles_attempted':   att,
            'dribbles_won':         succ,
            'dribbles_lost':        (att - succ) if (att is not None and succ is not None) else None,
            'dribble_success_pct':  _to_num(r.get('Take-Ons_Succ%')),
            'dispossessed':         _to_int(r.get('Carries_Dis')),
        })
        rows.append(base)
    return rows


def build_carries(frames, combo_id, ws_match_id, team_id, ident):
    rows = []
    df = frames.get('Possession')
    if df is None:
        return rows
    for _, r in df.iterrows():
        base, pid = _base(r, combo_id, ws_match_id, team_id, ident)
        if pid is None:
            continue
        base.update({
            'carries':                  _to_int(r.get('Carries_Carries')),
            'carry_distance_m':         _yd_to_m(r.get('Carries_TotDist')),
            'progressive_carries':      _to_int(r.get('Carries_PrgC')),
            'carry_final_third_entries': _to_int(r.get('Carries_1/3')),
        })
        rows.append(base)
    return rows


def build_defending(frames, combo_id, ws_match_id, team_id, ident):
    rows = []
    df = frames.get('Defense')
    misc = frames.get('Miscellaneous')
    misc_by_player = {}
    if misc is not None:
        for _, r in misc.iterrows():
            misc_by_player[normalize_name(r['Player'])] = r
    if df is None:
        return rows
    for _, r in df.iterrows():
        base, pid = _base(r, combo_id, ws_match_id, team_id, ident)
        if pid is None:
            continue
        mr = misc_by_player.get(normalize_name(r['Player']))
        won = _to_int(mr.get('Aerial Duels_Won')) if mr is not None else None
        lost = _to_int(mr.get('Aerial Duels_Lost')) if mr is not None else None
        err = _to_int(r.get('Err'))
        base.update({
            'tackles_total':        _to_int(r.get('Tackles_Tkl')),
            'tackles_won':          _to_int(r.get('Tackles_TklW')),
            'tackles_own_third':    _to_int(r.get('Tackles_Def 3rd')),
            'tackles_middle_third': _to_int(r.get('Tackles_Mid 3rd')),
            'tackles_final_third':  _to_int(r.get('Tackles_Att 3rd')),
            'dribbled_past':        _to_int(r.get('Challenges_Lost')),
            'interceptions':        _to_int(r.get('Int')),
            'clearances':           _to_int(r.get('Clr')),
            'blocked_shots':        _to_int(r.get('Blocks_Sh')),
            'blocked_passes':       _to_int(r.get('Blocks_Pass')),
            'errors_leading_to_shot': err,
            'errors_total':         err,
            'aerials_won':          won,
            'aerials_lost':         lost,
            'aerials_total':        (won + lost) if (won is not None and lost is not None) else None,
            'aerial_win_pct':       _to_num(mr.get('Aerial Duels_Won%')) if mr is not None else None,
            'ball_recoveries':      _to_int(mr.get('Performance_Recov')) if mr is not None else None,
            'fouls_conceded':       _to_int(mr.get('Performance_Fls')) if mr is not None else None,
        })
        rows.append(base)
    return rows


def build_discipline(frames, combo_id, ws_match_id, team_id, ident):
    rows = []
    df = frames.get('Miscellaneous')
    if df is None:
        return rows
    for _, r in df.iterrows():
        base, pid = _base(r, combo_id, ws_match_id, team_id, ident)
        if pid is None:
            continue
        base.update({
            'fouls_committed':     _to_int(r.get('Performance_Fls')),
            'fouls_won':           _to_int(r.get('Performance_Fld')),
            'yellow_cards':        _to_int(r.get('Performance_CrdY')),
            'red_cards':           _to_int(r.get('Performance_CrdR')),
            'second_yellow_cards': _to_int(r.get('Performance_2CrdY')),
            'offsides_committed':  _to_int(r.get('Performance_Off')),
        })
        rows.append(base)
    return rows


def build_goalkeep(frames, combo_id, ws_match_id, team_id, ident):
    rows = []
    df = frames.get('Keeper')
    if df is None:
        return rows
    for _, r in df.iterrows():
        base, pid = _base(r, combo_id, ws_match_id, team_id, ident)
        if pid is None:
            continue
        base.update({
            'saves':                 _to_int(r.get('Shot Stopping_Saves')),
            'goals_conceded':        _to_int(r.get('Shot Stopping_GA')),
            'shots_on_target_faced': _to_int(r.get('Shot Stopping_SoTA')),
            'save_pct':              _to_num(r.get('Shot Stopping_Save%')),
            'goal_kicks':            _to_int(r.get('Goal Kicks_Att')),
            'keeper_throws':         _to_int(r.get('Passes_Thr')),
            'passes_total':          _to_int(r.get('Passes_Att (GK)')),
            'longballs':             _to_int(r.get('Launched_Att')),
            'longballs_accurate':    _to_int(r.get('Launched_Cmp')),
            'longball_accuracy_pct': _to_num(r.get('Launched_Cmp%')),
            'avg_distribution_length_m': _yd_to_m(r.get('Passes_AvgLen')),
            'long_kick_pct':         _to_num(r.get('Passes_Launch%')),
            'sweeper_actions':       _to_int(r.get('Sweeper_#OPA')),
        })
        rows.append(base)
    return rows


# outcome → boolean flags for ws_shots
_ON_TARGET = {'Goal', 'Saved', 'Save'}
_OFF_TARGET = {'Off Target', 'Wide', 'Missed', 'Off T', 'Woodwork', 'Post', 'Bar'}


def build_shots(sheets, combo_id, ws_match_id, team_for_squad, ident):
    """
    ws_shots from the shared Shot_Data sheet. row_id is the chronological index
    (the cross-provider shot key) — same convention as the other providers.
    """
    df = sheets.get('Shot_Data')
    if df is None:
        return []
    # sort chronologically by parsed minute to assign row_id
    recs = []
    for _, r in df.iterrows():
        base_min, exp_min = _parse_minute(r.get('Minute'))
        recs.append((base_min if base_min is not None else 9999, exp_min, r))
    recs.sort(key=lambda t: (t[0], t[1] if t[1] is not None else 0))

    rows = []
    for row_id, (base_min, exp_min, r) in enumerate(recs):
        squad = _clean(r.get('Squad'))
        team_id = team_for_squad(squad)
        pid = ident.player_id(r.get('Player'), team_id) if team_id is not None else None
        outcome = _clean(r.get('Outcome'))
        body = _clean(r.get('Body Part'))
        notes = _clean(r.get('Notes')) or ''
        sca1_id = ident.player_id(r.get('SCA 1_Player'), team_id) if (team_id is not None and _clean(r.get('SCA 1_Player'))) else None
        sca2_id = ident.player_id(r.get('SCA 2_Player'), team_id) if (team_id is not None and _clean(r.get('SCA 2_Player'))) else None
        rows.append({
            'combo_id': combo_id,
            'whoscored_match_id': ws_match_id,
            'row_id': row_id,
            'event_id': None,
            'team_id': team_id,
            'player_id': pid,
            'player_name': _player_name(r.get('Player')),
            'minute': base_min if base_min != 9999 else None,
            'second': None,
            'expanded_minute': exp_min,
            'period': None,
            'x': None, 'y': None,
            'is_goal':       outcome == 'Goal',
            'is_on_target':  outcome in _ON_TARGET,
            'is_blocked':    outcome == 'Blocked',
            'is_off_target': outcome in _OFF_TARGET,
            'is_open_play': None,
            'location': None, 'in_box': None, 'six_yard_box': None, 'small_box': None,
            'distance_m':  _yd_to_m(r.get('Distance')),
            'angle_deg': None,
            'body_part': body,
            'is_header':     body == 'Head',
            'is_right_foot': body == 'Right Foot',
            'is_left_foot':  body == 'Left Foot',
            'is_volley':     'Volley' in notes,
            'is_first_touch': None,
            'pattern': None,
            'is_from_corner': None, 'is_from_freekick': None, 'is_fast_break': None,
            'is_assisted':   _clean(r.get('SCA 1_Player')) is not None,
            'is_individual_play': None, 'is_big_chance': None, 'is_one_on_one': None,
            'goal_mouth_y': None, 'goal_mouth_z': None, 'placement': None,
            'related_event_id': None,
            'xgot': _to_num(r.get('PSxG')),          # post-shot xG ≈ xGOT
            'sca_1_player_id': sca1_id, 'sca_1_player_name': _player_name(r.get('SCA 1_Player')), 'sca_1_type': _clean(r.get('SCA 1_Event')),
            'sca_2_player_id': sca2_id, 'sca_2_player_name': _player_name(r.get('SCA 2_Player')), 'sca_2_type': _clean(r.get('SCA 2_Event')),
        })
    return rows


# ──────────────────────────────────────────────────────────────────────────
#  orchestration
# ──────────────────────────────────────────────────────────────────────────
# (target table, builder, takes per-team frames?)
PLAYER_TABLES = [
    ('ws_player_shooting',     build_shooting),
    ('ws_player_passing',      build_passing),
    ('ws_player_passing_adv',  build_passing_adv),
    ('ws_player_possession',   build_possession),
    ('ws_player_carries',      build_carries),
    ('ws_player_defending',    build_defending),
    ('ws_player_discipline',   build_discipline),
    ('ws_player_goalkeep',     build_goalkeep),
]


def build_all(sheets, combo_id, ws_match_id, home_team, away_team, ident):
    """
    home_team/away_team: dicts {team_id, team_name, squad} for the two sides.
    Returns {table_name: [row dicts]} for every target table.
    """
    out = {'ws_match_meta': build_match_meta(combo_id, ws_match_id, home_team, away_team),
           'ws_lineups': []}
    for tbl, _ in PLAYER_TABLES:
        out[tbl] = []

    for side, info, is_home in (('Home', home_team, True), ('Away', away_team, False)):
        frames = _team_frames(sheets, side)
        tid = info['team_id']
        out['ws_lineups'] += build_lineups(
            frames, combo_id, ws_match_id, tid, info['team_name'], is_home, ident)
        for tbl, fn in PLAYER_TABLES:
            out[tbl] += fn(frames, combo_id, ws_match_id, tid, ident)

    # shots: resolve each row's team from its Squad string
    squad_to_team = {}
    for info in (home_team, away_team):
        squad_to_team[normalize_name(info['squad'])] = info['team_id']

    def team_for_squad(squad):
        tn = normalize_name(squad)
        if tn in squad_to_team:
            return squad_to_team[tn]
        for name, tid in squad_to_team.items():
            if tn and (tn in name or name in tn):
                return tid
        return None

    out['ws_shots'] = build_shots(sheets, combo_id, ws_match_id, team_for_squad, ident)
    return out


def write_tables(conn, schema, tables, dry_run=False):
    """Idempotent delete-by-combo_id then insert for each target table."""
    cur = conn.cursor() if not dry_run else None
    summary = {}
    for tbl, rows in tables.items():
        rows = [r for r in rows if r.get('player_id') is not None] if tbl not in ('ws_shots', 'ws_match_meta') else rows
        summary[tbl] = len(rows)
        if dry_run or not rows:
            continue
        combo_id = rows[0]['combo_id']
        cur.execute(f'DELETE FROM {schema}.{tbl} WHERE combo_id = %s', (combo_id,))
        cols = list(rows[0].keys())
        collist = ', '.join(cols)
        ph = ', '.join(['%s'] * len(cols))
        vals = [tuple(r[c] for c in cols) for r in rows]
        cur.executemany(
            f'INSERT INTO {schema}.{tbl} ({collist}) VALUES ({ph})', vals)
    if not dry_run:
        conn.commit()
        cur.close()
    return summary


def backfill_match(conn, xlsx_path, combo_id, ws_match_id,
                   home_squad, away_squad, schema='raw', dry_run=False,
                   roster=None):
    """
    Top-level entry. Resolves identity, maps every sheet, writes the WhoScored
    summary tables + ws_shots for `combo_id`. Never writes ws_match_meta, so
    has_ws_events stays false for this match.
    """
    sheets = read_workbook(xlsx_path)
    ident = Identity(roster) if roster is not None else Identity.from_db(
        conn, schema, [home_squad, away_squad])

    home_tid = ident.team_id(home_squad)
    away_tid = ident.team_id(away_squad)
    if home_tid is None or away_tid is None:
        raise ValueError(
            f'[{combo_id}] could not resolve team_id for '
            f'{[s for s,t in [(home_squad,home_tid),(away_squad,away_tid)] if t is None]} '
            f'— check squad names against ws_lineups.team_name')

    home = {'team_id': home_tid, 'team_name': home_squad, 'squad': home_squad}
    away = {'team_id': away_tid, 'team_name': away_squad, 'squad': away_squad}
    tables = build_all(sheets, combo_id, ws_match_id, home, away, ident)
    summary = write_tables(conn, schema, tables, dry_run=dry_run)

    if ident.unresolved_players:
        log.warning(f'[{combo_id}] {len(ident.unresolved_players)} unresolved '
                    f'players (skipped): '
                    f'{sorted(set(p for p,_ in ident.unresolved_players))}')
    log.info(f'[{combo_id}] FBref backfill {"(dry-run) " if dry_run else ""}'
             f'mapped: ' + ', '.join(f'{k}={v}' for k, v in summary.items()))
    return summary, ident


# ──────────────────────────────────────────────────────────────────────────
#  standalone runner  (edit CONFIG, then `python fbref_backfill.py`)
# ──────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import psycopg2

    # ===================== CONFIG — edit per match =========================
    SOURCE_PATH = r'/path/to/fbref_csvs_or_combined.xlsx'  # folder of CSVs OR an .xlsx
    COMBO_ID   = '2021-09-18CryNor'          # the match's combo_id on disk
    WS_MATCH_ID = 1549733                     # WhoScored match id (for reference)
    HOME_SQUAD = 'Crystal Palace'             # must match ws_lineups.team_name
    AWAY_SQUAD = 'Norwich City'
    SCHEMA     = 'raw'
    DRY_RUN    = True                         # True = map + report, write nothing

    # warehouse connection (the_data_xi postgres)
    PG_HOST = '127.0.0.1'
    PG_PORT = 4500
    PG_DB   = 'the_data_xi'
    PG_USER = 'the_data_xi'
    PG_PASS = 'the_data_xi'
    # =======================================================================

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                            user=PG_USER, password=PG_PASS)
    try:
        summary, ident = backfill_match(
            conn, SOURCE_PATH, COMBO_ID, WS_MATCH_ID,
            HOME_SQUAD, AWAY_SQUAD, schema=SCHEMA, dry_run=DRY_RUN)
        print('\nMapped row counts:')
        for k, v in summary.items():
            print(f'  {k:24} {v}')
        if ident.unresolved_teams:
            print('\nUNRESOLVED TEAMS (fix squad names):', ident.unresolved_teams)
        if ident.unresolved_players:
            miss = sorted(set(p for p, _ in ident.unresolved_players))
            print(f'\n{len(miss)} unresolved players (skipped):')
            for p in miss:
                print(f'  - {p}')
        print(f'\n{"DRY RUN — nothing written." if DRY_RUN else "Written and committed."}')
    finally:
        conn.close()
