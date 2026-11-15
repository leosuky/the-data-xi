"""Tests for include/helpers/whoscored/parse_shot_xg.py (Data XI xG / xGOT).

The synthetic tests run anywhere with lightgbm installed. The reference-match
test runs where the raw match files are (the Airflow container,
/usr/local/airflow/data) and checks the parser against the research output.

    pytest tests/helpers/test_parse_shot_xg.py
"""

import glob
import json
import os

import pytest

from include.helpers.whoscored.parse_shot_xg import PENALTY_XG, parse_shot_xg

HOME, AWAY = 1, 2
GK_HOME, GK_AWAY, SUB_GK_AWAY, OUTFIELD_AWAY = 10, 20, 21, 22


def _event(i, team, name, minute, second=0, period=1, player=None, x=50.0, y=50.0, quals=(), **extra):
    return {'id': i, 'eventId': i, 'teamId': team, 'playerId': player, 'minute': minute, 'second': second,
            'period': {'value': period}, 'type': {'displayName': name}, 'outcomeType': {'value': 1},
            'x': x, 'y': y, 'qualifiers': [{'type': {'displayName': q}} for q in quals], **extra}


def _match(events):
    players = lambda gk, team: [{'playerId': gk, 'position': 'GK', 'isFirstEleven': True}] + [
        {'playerId': team * 100 + k, 'position': 'FW', 'isFirstEleven': True} for k in range(10)]
    return {'home': {'teamId': HOME, 'players': players(GK_HOME, HOME)},
            'away': {'teamId': AWAY, 'players': players(GK_AWAY, AWAY) + [
                {'playerId': SUB_GK_AWAY, 'position': 'Sub', 'isFirstEleven': False}]},
            'events': events}


def _shots(events):
    return {(r['team_id'], r['event_id']): r for r in parse_shot_xg(_match(events), 999)['shots']}


def test_kinds_of_shot():
    ev = [
        _event(1, HOME, 'Pass', 10, x=80, y=50, endX=94, endY=50, quals=('Throughball',)),
        _event(2, HOME, 'Goal', 10, 5, player=101, x=94, y=50, goalMouthY=51, goalMouthZ=5,
               quals=('RightFoot', 'Assisted'), relatedEventId=1),
        _event(3, HOME, 'SavedShot', 20, player=102, x=88.6, y=50, goalMouthY=48, goalMouthZ=10,
               quals=('Penalty', 'RightFoot')),
        _event(4, HOME, 'SavedShot', 30, player=103, x=80, y=40, goalMouthY=50, goalMouthZ=5,
               quals=('Blocked', 'RightFoot')),
        _event(5, HOME, 'MissedShots', 40, player=104, x=75, y=60, goalMouthY=56, goalMouthZ=20),
        _event(6, AWAY, 'Goal', 50, player=201, x=97, y=50, goalMouthY=50, goalMouthZ=3, quals=('OwnGoal',)),
    ]
    s = _shots(ev)
    assert len(s) == 5

    goal = s[(HOME, 2)]
    assert goal['is_goal'] and goal['is_on_target'] and goal['keeper_id'] == GK_AWAY
    assert 0.2 < goal['dxi_xg'] < 0.9 and 0 < goal['dxi_xgot'] <= 1
    assert goal['model_version'] and goal['whoscored_match_id'] == 999

    pen = s[(HOME, 3)]
    assert pen['is_penalty'] and pen['dxi_xg'] == PENALTY_XG and 0 < pen['dxi_xgot'] < 1

    blocked, missed = s[(HOME, 4)], s[(HOME, 5)]
    assert not blocked['is_on_target'] and blocked['dxi_xgot'] == 0
    assert not missed['is_on_target'] and missed['dxi_xgot'] == 0

    og = s[(AWAY, 6)]
    assert og['is_own_goal'] and not og['is_goal'] and not og['is_on_target']
    assert og['dxi_xg'] is None and og['dxi_xgot'] is None


def test_score_state_counts_own_goal_for_the_other_side():
    ev = [
        _event(1, AWAY, 'Goal', 5, player=201, x=97, y=50, goalMouthY=50, goalMouthZ=3, quals=('OwnGoal',)),
        _event(2, HOME, 'MissedShots', 10, player=101, x=85, y=50, goalMouthY=56, goalMouthZ=20),
        _event(3, AWAY, 'MissedShots', 10, player=202, x=85, y=50, goalMouthY=56, goalMouthZ=20),
    ]
    # same shot, opposite score state (home +1, away -1): different xG
    s = _shots(ev)
    assert s[(HOME, 2)]['dxi_xg'] != s[(AWAY, 3)]['dxi_xg']


def test_same_event_id_for_both_teams():
    # WhoScored numbers events per team: both teams can have event 7
    ev = [_event(7, HOME, 'MissedShots', 10, player=101, x=85, y=50, goalMouthY=56, goalMouthZ=20),
          _event(7, AWAY, 'SavedShot', 11, player=201, x=90, y=50, goalMouthY=50, goalMouthZ=10)]
    s = _shots(ev)
    assert set(s) == {(HOME, 7), (AWAY, 7)}
    assert s[(AWAY, 7)]['keeper_id'] == GK_HOME and s[(HOME, 7)]['keeper_id'] == GK_AWAY


def test_keeper_substitution_and_red_card():
    shot = lambda i, minute: _event(i, HOME, 'SavedShot', minute, player=101, x=90, y=50, goalMouthY=50, goalMouthZ=10)
    ev = [
        shot(1, 10),
        _event(2, AWAY, 'SubstitutionOff', 20, player=GK_AWAY, relatedPlayerId=SUB_GK_AWAY),
        shot(3, 25),
        _event(4, AWAY, 'Card', 30, player=SUB_GK_AWAY, cardType={'displayName': 'Red'}),
        shot(5, 31),
        _event(6, AWAY, 'KeeperPickup', 35, player=OUTFIELD_AWAY),
        shot(7, 40),
    ]
    s = _shots(ev)
    assert s[(HOME, 1)]['keeper_id'] == GK_AWAY
    assert s[(HOME, 3)]['keeper_id'] == SUB_GK_AWAY
    # after the red card: the first player to make a keeper action, from the red card on
    assert s[(HOME, 5)]['keeper_id'] == OUTFIELD_AWAY
    assert s[(HOME, 7)]['keeper_id'] == OUTFIELD_AWAY


def test_no_shots():
    assert parse_shot_xg(_match([_event(1, HOME, 'Pass', 1)]), 1) == {'shots': []}


# ── Reference matches (research output, Nov 2026) ─────────────────────────────

DATA = os.environ.get('DXI_DATA_DIR', '/usr/local/airflow/data')
REFERENCE = [
    # competition/season, date, team abbreviations, shots, on target, xG, xGOT, goals (incl. own goals)
    ('Premier_League/2018_2019', '2018-08-18', ('Che', 'Ars'), 39, 17, 4.49, 6.11, 5),
    ('Serie_A/2023_2024', '2023-08-26', ('Rom',), 28, 10, 2.44, 3.17, 3),
    ('EFL_Championship/2026_2027', '2026-08-17', ('Car', 'Wre'), 26, 10, 2.45, 3.08, 2),
]


@pytest.mark.parametrize('season, date, teams, shots, on_target, xg, xgot, goals', REFERENCE)
def test_reference_match(season, date, teams, shots, on_target, xg, xgot, goals):
    folders = [f for f in glob.glob(os.path.join(DATA, season, f'{date}*'))
               if all(t in os.path.basename(f) for t in teams)]
    files = glob.glob(os.path.join(folders[0], 'whoscored', 'whoscored_*.json')) if folders else []
    if not files:
        pytest.skip(f'no raw file for {season} {date}')
    with open(files[0]) as f:
        rows = parse_shot_xg(json.load(f), 0)['shots']
    assert len(rows) == shots
    assert sum(r['is_on_target'] for r in rows) == on_target
    assert sum(r['is_goal'] or r['is_own_goal'] for r in rows) == goals
    assert round(sum(r['dxi_xg'] or 0 for r in rows), 2) == pytest.approx(xg, abs=0.01)
    assert round(sum(r['dxi_xgot'] or 0 for r in rows), 2) == pytest.approx(xgot, abs=0.01)
