"""
parse_shot_xg.py
----------------
Data XI xG and xGOT for every shot of a match, from WhoScored matchCentreData.

One row per shot (periods 1-4; shoot-outs dropped) for raw.ws_shot_xg:
    dxi_xg       chance of scoring before the shot was taken (boosted model)
    dxi_xg_l1    the same from the logistic fallback (numpy only)
    dxi_xgot     chance a shot ON TARGET goes in, from where it was heading (boosted)
    dxi_xgot_l1  the same from the logistic fallback
    is_on_target Goal, or SavedShot without the Blocked qualifier
    keeper_id    WhoScored id of the defending keeper at the moment of the shot

Models: fitted by the xG / xGOT research runs (Nov 2026; reports
xg_model_2026-11-12.md and xgot_model_2026-11-13.md) on top-five shots
2018/19 to 2 Nov 2026. The files in ./models are copies of the research
output; refit yearly and bump MODEL_VERSION (see documentation).

RULES (each one cost a wrong result in the research — keep them)
-----------------------------------------------------------------
  1. Events sorted by period, minute, second, id; periods 1-4 only.
  2. Only the shot event and the event WhoScored links to it as the assist
     (relatedEventId, same team). Nothing from neighbouring events: a shot's
     own save can be listed before it.
  3. Nothing derived from the shot's timestamp: from 2020/21 a goal is
     stamped when the ball crosses the line.
  4. No BigChance, FastBreak, FirstTouch, Volley, OneOnOne, IndividualPlay,
     ThrowinSetPiece: their use changes by season.
  5. No distance from the pass end point to the shot (WhoScored changed how
     pass end points are recorded in Sep 2025).
  6. Score and red-card state from earlier events of the same match (own goals
     credited to the other side).
  7. Penalties: xG 0.79 flat; xGOT from the penalty placement model (0 off
     target). Own goals: no xG, no xGOT, not on target.
  8. Off target (blocked — incl. saved off the line —, post, miss): xGOT 0.
     A blocked shot's goal-mouth position is never used.
  9. Features go to the boosted models in the order of the meta files, float32.

KEEPER
------
The defending team's starting GK; after he is substituted, the player who
came on; after a red card to the keeper, the first player of that team to
make a keeper action (pickup, claim, punch, smother, sweeper) afterwards,
from the moment of the red card. NULL if nobody does.

Join key: WhoScored numbers events per team, so a shot is identified by
(combo_id, team_id, event_id) — never (combo_id, event_id) alone.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

MODEL_VERSION = '2026-11'
MODEL_DIR = Path(__file__).resolve().parent / 'models'

PENALTY_XG = 0.79
SHOT_TYPES = {'MissedShots', 'SavedShot', 'ShotOnPost', 'Goal'}
KEEPER_ACTIONS = {'KeeperPickup', 'Claim', 'Punch', 'Smother', 'KeeperSweeper'}

FLAGS = ['head', 'other_body', 'from_corner', 'set_piece', 'direct_fk', 'assisted',
         'a_cross', 'a_through', 'a_long', 'a_head', 'a_chipped', 'a_layoff',
         'a_corner', 'a_fk', 'a_cutback', 'in_box', 'six_yard']
NUM = ['dist', 'angle', 'abs_dy', 'dx', 'a_len', 'score_diff', 'red_diff']
FEATURES = FLAGS + NUM
PLACE = ['gm_off', 'gm_z', 'gm_side', 'shot_side', 'far_post']

_MODELS = {}


# ── Models ────────────────────────────────────────────────────────────────────

def _models() -> dict:
    """Load the model files once per process."""
    if not _MODELS:
        import lightgbm as lgb

        def js(name):
            with open(MODEL_DIR / name) as f:
                return json.load(f)

        xg_meta, xgot_meta = js('xg_meta.json'), js('xgot_meta.json')
        if xg_meta['features'] != FEATURES or xgot_meta['features'] != PLACE + FEATURES:
            raise ValueError('shot_xg: model feature order does not match the parser')
        _MODELS.update(
            xg=lgb.Booster(model_file=str(MODEL_DIR / 'xg_g1.txt')),
            xgot=lgb.Booster(model_file=str(MODEL_DIR / 'xgot_g2.txt')),
            xg_l1=js('xg_l1.json'), xgot_l1=js('xgot_l1.json'), xgot_pen=js('xgot_pen.json'),
        )
    return _MODELS


def _logistic(columns: dict, params: dict) -> np.ndarray:
    X = np.column_stack([columns[k] for k in params['columns']])
    z = ((X - np.array(params['mean'])) / np.array(params['scale'])) @ np.array(params['coef']) + params['intercept']
    return 1 / (1 + np.exp(-z))


# ── Shots ─────────────────────────────────────────────────────────────────────

def _quals(e: dict) -> str:
    return '|'.join(sorted(q['type']['displayName'] for q in e.get('qualifiers', ())))


def _sorted_events(data: dict) -> list:
    ev = [e for e in data.get('events', []) if e['period']['value'] in (1, 2, 3, 4)]
    ev.sort(key=lambda e: (e['period']['value'], e['minute'], e.get('second') or 0, e.get('id', 0)))
    return ev


def _shots(data: dict) -> list:
    """Every shot with its linked assist and the match state before it."""
    ev = _sorted_events(data)
    home, away = data['home']['teamId'], data['away']['teamId']
    by_id = {(e['teamId'], e['eventId']): e for e in ev if 'eventId' in e and 'teamId' in e}
    score, reds = {home: 0, away: 0}, {home: 0, away: 0}
    rows = []
    for e in ev:
        name, team = e['type']['displayName'], e.get('teamId')
        if team not in score:
            continue
        opp = away if team == home else home
        if name == 'Card' and (e.get('cardType') or {}).get('displayName') in ('Red', 'SecondYellow'):
            reds[team] += 1
        if name not in SHOT_TYPES:
            continue
        q = _quals(e)
        r = {'event_id': e.get('eventId'), 'team_id': team, 'opp_id': opp, 'player_id': e.get('playerId'),
             'period': e['period']['value'], 'minute': e['minute'], 'second': e.get('second'),
             'x': e['x'], 'y': e['y'], 'goal_mouth_y': e.get('goalMouthY'), 'goal_mouth_z': e.get('goalMouthZ'),
             'type': name, 'quals': q,
             'score_for': score[team], 'score_against': score[opp],
             'reds_for': reds[team], 'reds_against': reds[opp]}
        a = by_id.get((team, e.get('relatedEventId'))) if e.get('relatedEventId') is not None else None
        if a is not None:
            r.update(a_type=a['type']['displayName'], a_x=a['x'], a_y=a['y'],
                     a_ex=a.get('endX'), a_ey=a.get('endY'), a_quals=_quals(a))
        rows.append(r)
        if name == 'Goal':
            score[opp if 'OwnGoal' in q.split('|') else team] += 1
    return rows


def _has(s: pd.Series, q: str) -> np.ndarray:
    return s.fillna('').str.contains(rf'(?:^|\|){q}(?:\||$)', regex=True).to_numpy()


def _features(raw: pd.DataFrame) -> pd.DataFrame:
    """The 24 xG features (pitch 105 x 68 m, goal 7.32 m) plus pen / own_goal flags."""
    for c in ('a_type', 'a_quals', 'a_x', 'a_y', 'a_ex', 'a_ey'):
        if c not in raw:
            raw = raw.assign(**{c: np.nan})
    q = raw.quals
    aq = pd.Series(raw.a_quals.astype(object).where(raw.a_quals.notna(), ''), index=raw.index).astype(str)
    o = pd.DataFrame(index=raw.index)
    dx, dy = (100 - raw.x.to_numpy(float)) * 1.05, (raw.y.to_numpy(float) - 50) * 0.68
    o['dist'] = np.hypot(dx, dy)
    ang = np.arctan2(7.32 * dx, dx ** 2 + dy ** 2 - 3.66 ** 2)
    o['angle'] = np.where(ang < 0, ang + np.pi, ang)
    o['abs_dy'], o['dx'] = np.abs(dy), dx
    o['in_box'], o['six_yard'] = (dx <= 16.5) & (np.abs(dy) <= 20.16), (dx <= 5.5) & (np.abs(dy) <= 9.16)
    o['head'], o['other_body'] = _has(q, 'Head'), _has(q, 'OtherBodyPart')
    o['from_corner'], o['set_piece'] = _has(q, 'FromCorner'), _has(q, 'SetPiece')
    o['direct_fk'], o['assisted'] = _has(q, 'DirectFreekick'), _has(q, 'Assisted')
    linked = raw.a_type.notna().to_numpy()
    for name, qn in (('a_cross', 'Cross'), ('a_through', 'Throughball'), ('a_long', 'Longball'),
                     ('a_head', 'HeadPass'), ('a_chipped', 'Chipped'), ('a_layoff', 'LayOff'),
                     ('a_corner', 'CornerTaken')):
        o[name] = _has(aq, qn) & linked
    o['a_fk'] = (_has(aq, 'FreekickTaken') | _has(aq, 'IndirectFreekickTaken')) & linked
    ax, ay, aex, aey = (raw[c].to_numpy(float) for c in ('a_x', 'a_y', 'a_ex', 'a_ey'))
    with np.errstate(invalid='ignore'):
        o['a_cutback'] = linked & (ax >= 88) & (aex < ax - 2) & (np.abs(ay - 50) > 10) & ~o.a_corner.to_numpy()
        o['a_len'] = np.where(linked, np.hypot((aex - ax) * 1.05, (aey - ay) * 0.68), np.nan)
    o['score_diff'] = (raw.score_for - raw.score_against).clip(-3, 3).to_numpy(float)
    o['red_diff'] = (raw.reds_against - raw.reds_for).clip(-2, 2).to_numpy(float)
    o['pen'], o['own_goal'] = _has(q, 'Penalty'), _has(q, 'OwnGoal')
    return o


def _placement(raw: pd.DataFrame) -> pd.DataFrame:
    """Where the shot was heading in the goal (WhoScored goal: y 45.2-54.8, z 0-38)."""
    gy, sy = raw.goal_mouth_y.to_numpy(float) - 50.0, raw.y.to_numpy(float) - 50.0
    return pd.DataFrame({
        'gm_off': np.abs(gy), 'gm_z': raw.goal_mouth_z.to_numpy(float), 'gm_side': gy, 'shot_side': sy,
        'far_post': np.sign(gy) * np.sign(sy) * np.minimum(np.abs(gy), 4.8) * np.minimum(np.abs(sy), 20) / 20,
    }, index=raw.index)


# ── Logistic fallbacks ────────────────────────────────────────────────────────

def _xg_l1(F: pd.DataFrame, params: dict) -> np.ndarray:
    dist, ang, head = F.dist.to_numpy(float), F.angle.to_numpy(float), F['head'].to_numpy(float)
    c = {'dist': dist / 10, 'dist2': (dist / 10) ** 2, 'angle': ang, 'head': head, 'logdist': np.log1p(dist),
         'angle2': ang ** 2, 'dist_x_angle': dist / 10 * ang, 'head_x_dist': head * dist / 10, 'head_x_angle': head * ang}
    for k in (6.0, 11.0, 16.5, 25.0):
        c[f'd_gt_{k}'] = np.clip(dist - k, 0, None) / 10
    for f in FLAGS:
        if f != 'head':
            c[f] = F[f].to_numpy(float)
    c.update({'a_len': np.nan_to_num(F.a_len.to_numpy(float), nan=0.0) / 20,
              'head_x_cross': head * F.a_cross.to_numpy(float),
              'fk_x_dist': F.direct_fk.to_numpy(float) * dist / 10,
              'through_x_dist': F.a_through.to_numpy(float) * dist / 10,
              'score_diff': F.score_diff.to_numpy(float), 'red_diff': F.red_diff.to_numpy(float)})
    return _logistic(c, params)


def _xgot_l1(F: pd.DataFrame, P: pd.DataFrame, xg: np.ndarray, params: dict) -> np.ndarray:
    xg = np.clip(xg, 1e-4, 1 - 1e-4)
    lx = np.log(xg / (1 - xg))
    off, z = P.gm_off.to_numpy(float), P.gm_z.to_numpy(float)
    head, dist = F['head'].to_numpy(float), F.dist.to_numpy(float)
    c = {'lxg': lx, 'off': off, 'off2': off ** 2, 'z': z / 10, 'z2': (z / 10) ** 2,
         'off_x_z': off * z / 10, 'off2_x_z2': off ** 2 * (z / 10) ** 2}
    for k in (1.5, 3.0, 4.0):
        c[f'off_gt_{k}'] = np.clip(off - k, 0, None)
    for k in (6.0, 15.0, 25.0, 32.0):
        c[f'z_gt_{k}'] = np.clip(z - k, 0, None) / 10
    c.update({'head': head, 'other_body': F.other_body.to_numpy(float), 'logdist': np.log1p(dist),
              'dist_x_off': dist / 10 * off, 'dist_x_z': dist / 10 * z / 10,
              'head_x_off': head * off, 'head_x_z': head * z / 10,
              'lxg_x_off': lx * off, 'lxg_x_z': lx * z / 10, 'far_post': P.far_post.to_numpy(float)})
    return _logistic(c, params)


def _xgot_pen(P: pd.DataFrame, params: dict) -> np.ndarray:
    off, z = P.gm_off.to_numpy(float), P.gm_z.to_numpy(float) / 10
    z_ = np.c_[off, off ** 2, z, z ** 2, off * z] @ np.array(params['coef']) + params['intercept']
    return 1 / (1 + np.exp(-z_))


# ── Keeper in goal ────────────────────────────────────────────────────────────

def _keepers(data: dict) -> dict:
    """{team_id: [((period, seconds), keeper_id or None), ...]} — who is in goal from when."""
    ev = _sorted_events(data)
    when = lambda e: (e['period']['value'], e['minute'] * 60 + (e.get('second') or 0))
    out = {}
    for side in ('home', 'away'):
        team = data[side]
        gk = [p['playerId'] for p in team.get('players', []) if p.get('position') == 'GK' and p.get('isFirstEleven')]
        cur = gk[0] if gk else None
        spells = [((0, 0), cur)]
        sent_off_at = None
        for e in ev:
            if e.get('teamId') != team['teamId']:
                continue
            name = e['type']['displayName']
            if name == 'SubstitutionOff' and cur is not None and e.get('playerId') == cur:
                cur = e.get('relatedPlayerId')
                spells.append((when(e), cur))
            elif (name == 'Card' and cur is not None and e.get('playerId') == cur
                  and (e.get('cardType') or {}).get('displayName') in ('Red', 'SecondYellow')):
                cur, sent_off_at = None, when(e)
                spells.append((sent_off_at, None))
            elif name in KEEPER_ACTIONS and cur is None and sent_off_at is not None and e.get('playerId'):
                cur = e['playerId']
                spells.append((sent_off_at, cur))
                sent_off_at = None
        out[team['teamId']] = spells
    return out


def _keeper_at(spells: list, period: int, seconds: int):
    cur = None
    for at, keeper in spells:
        if (period, seconds) >= at:
            cur = keeper
    return cur


# ── Public ────────────────────────────────────────────────────────────────────

def _native(v):
    """numpy scalars -> plain Python for psycopg2; NaN -> None."""
    if v is None:
        return None
    if isinstance(v, (np.floating, float)):
        return None if np.isnan(v) else float(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.bool_):
        return bool(v)
    return v


def parse_shot_xg(data: dict, whoscored_match_id: int) -> dict:
    """
    Data XI xG and xGOT for every shot of one match.

    Returns:
        {'shots': [...]}   one row per shot for raw.ws_shot_xg
    """
    raw = pd.DataFrame(_shots(data))
    if raw.empty:
        return {'shots': []}
    m = _models()
    F, P = _features(raw), _placement(raw)
    pen, og = F.pen.to_numpy(), F.own_goal.to_numpy()

    # xG
    xg = m['xg'].predict(F[FEATURES].astype(np.float32))
    xg_l1 = _xg_l1(F, m['xg_l1'])
    xg = np.where(pen, PENALTY_XG, np.where(og, np.nan, xg)).round(4)
    xg_l1 = np.where(pen, PENALTY_XG, np.where(og, np.nan, xg_l1)).round(4)

    # xGOT
    q = '|' + raw.quals.fillna('') + '|'
    on = ((raw.type == 'Goal') | ((raw.type == 'SavedShot') & ~q.str.contains('|Blocked|', regex=False))).to_numpy() & ~og
    xgot, xgot_l1 = np.zeros(len(raw)), np.zeros(len(raw))
    if on.any():
        X = pd.concat([P, F[FEATURES]], axis=1)[PLACE + FEATURES].astype(np.float32)
        xgot[on] = m['xgot'].predict(X[on])
        xgot_l1[on] = _xgot_l1(F[on], P[on], xg[on], m['xgot_l1'])
    k = on & pen
    if k.any():
        xgot[k] = xgot_l1[k] = _xgot_pen(P[k], m['xgot_pen'])
    xgot[og] = xgot_l1[og] = np.nan

    # keeper
    spells = _keepers(data)
    seconds = raw.minute.to_numpy() * 60 + raw.second.fillna(0).to_numpy()
    keeper = [_keeper_at(spells.get(o, []), p, s) for o, p, s in zip(raw.opp_id, raw.period, seconds)]

    rows = []
    for i in range(len(raw)):
        rows.append({k: _native(v) for k, v in {
            'whoscored_match_id': whoscored_match_id,
            'event_id':           raw.event_id.iat[i],
            'team_id':            raw.team_id.iat[i],
            'player_id':          raw.player_id.iat[i],
            'period':             raw.period.iat[i],
            'minute':             raw.minute.iat[i],
            'second':             raw.second.iat[i],
            'is_goal':            raw.type.iat[i] == 'Goal' and not og[i],
            'is_penalty':         pen[i],
            'is_own_goal':        og[i],
            'is_on_target':       on[i],
            'keeper_id':          keeper[i],
            'dxi_xg':             xg[i],
            'dxi_xg_l1':          xg_l1[i],
            'dxi_xgot':           round(float(xgot[i]), 4) if not np.isnan(xgot[i]) else None,
            'dxi_xgot_l1':        round(float(xgot_l1[i]), 4) if not np.isnan(xgot_l1[i]) else None,
            'model_version':      MODEL_VERSION,
        }.items()})
    return {'shots': rows}
