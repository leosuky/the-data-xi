"""
parse_lineups.py
----------------
Extracts lineup and formation data from WhoScored matchCentreData.

Produces:
    'player'     — one row per player with roster details, captain status,
                   shirt number, position, physical attributes, sub info.
    'formations' — one row per formation change per team (timeline of
                   tactical shifts during the match). Unique to WhoScored —
                   no other provider tracks in-game formation changes.

Captain is extracted from the formations data (captainPlayerId on the
starting formation for each team).
"""


def _get_captain_ids(data: dict) -> set[int]:
    """
    Extract captain player IDs from the formations data.
    Uses the FIRST formation entry for each team (the starting formation).
    """
    captains = set()
    for side in ('home', 'away'):
        formations = data[side].get('formations', [])
        if formations:
            cid = formations[0].get('captainPlayerId')
            if cid:
                captains.add(cid)
    return captains


def _parse_formation_changes(data: dict, whoscored_match_id: int, combo_id: str) -> list[dict]:
    """
    Extract formation timeline — one row per formation phase per team.

    Each row captures:
        - Which formation was used (e.g. '4231', '3421')
        - When it started and ended (expanded minutes)
        - Who was captain during that phase
        - Which sub triggered the change (if any)
        - Player positions within the formation

    This data is unique to WhoScored. No other provider tracks
    in-game formation changes with this level of detail.
    """
    rows = []
    names = data.get('playerIdNameDictionary', {})

    for side in ('home', 'away'):
        team = data[side]
        team_id = team['teamId']
        team_name = team['name']
        is_home = (side == 'home')

        for i, f in enumerate(team.get('formations', [])):
            rows.append({
                'whoscored_match_id':   whoscored_match_id,
                'combo_id':             combo_id,
                'team_id':              team_id,
                'team_name':            team_name,
                'is_home_team':         is_home,
                'formation_index':      i,
                'formation_name':       f.get('formationName'),
                'formation_id':         f.get('formationId'),
                'start_minute':         f.get('startMinuteExpanded'),
                'end_minute':           f.get('endMinuteExpanded'),
                'captain_player_id':    f.get('captainPlayerId'),
                'captain_name':         names.get(str(f.get('captainPlayerId', ''))),
                'sub_on_player_id':     f.get('subOnPlayerId'),
                'sub_off_player_id':    f.get('subOffPlayerId'),
                'player_ids':           f.get('playerIds', []),
                'jersey_numbers':       f.get('jerseyNumbers', []),
            })

    return rows


def parse_lineups(data: dict, whoscored_match_id: int, combo_id: str = '') -> dict:
    """
    Extract lineup and formation data from WhoScored matchCentreData.

    Args:
        data:                Raw WhoScored matchCentreData dict.
        whoscored_match_id:  WhoScored match ID (extracted from filename).
        combo_id:            Match combo_id (e.g. '2025-05-23ComInt').

    Returns:
        {
            'player': [
                {
                    'whoscored_match_id', 'combo_id', 'player_id', 'player_name',
                    'team_id', 'team_name', 'is_home_team', 'shirt_number',
                    'position', 'is_starter', 'is_captain', 'is_man_of_the_match',
                    'age', 'height_cm', 'weight_kg',
                    'subbed_in_minute', 'subbed_in_for',
                    'subbed_out_minute', 'subbed_out_for',
                }, ...
            ],
            'formations': [
                {
                    'whoscored_match_id', 'combo_id', 'team_id', 'team_name',
                    'is_home_team', 'formation_index', 'formation_name',
                    'start_minute', 'end_minute', 'captain_player_id',
                    'sub_on_player_id', 'sub_off_player_id',
                    'player_ids', 'jersey_numbers',
                }, ...
            ]
        }
    """
    captain_ids = _get_captain_ids(data)
    player_rows = []

    for side in ('home', 'away'):
        team = data[side]
        team_id = team['teamId']
        team_name = team['name']
        is_home = (side == 'home')

        for p in team.get('players', []):
            player_rows.append({
                'whoscored_match_id':    whoscored_match_id,
                'combo_id':              combo_id,
                'player_id':             p['playerId'],
                'player_name':           p['name'],
                'team_id':               team_id,
                'team_name':             team_name,
                'is_home_team':          is_home,
                'shirt_number':          p.get('shirtNo'),
                'position':              p.get('position'),
                'is_starter':            p.get('isFirstEleven', False),
                'is_captain':            p['playerId'] in captain_ids,
                'is_man_of_the_match':   p.get('isManOfTheMatch', False),
                'age':                   p.get('age'),
                'height_cm':             p.get('height'),
                'weight_kg':             p.get('weight'),
                'subbed_in_minute':      p.get('subbedInExpandedMinute'),
                'subbed_in_for':         p.get('subbedInPlayerId'),
                'subbed_out_minute':     p.get('subbedOutExpandedMinute'),
                'subbed_out_for':        p.get('subbedOutPlayerId'),
            })

    formation_rows = _parse_formation_changes(data, whoscored_match_id, combo_id)

    return {
        'player': player_rows,
        'formations': formation_rows,
    }


if __name__ == '__main__':
    import json

    path = './whoscored_1835212.json'
    ws_id = int(path.split('_')[-1].replace('.json', ''))

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    result = parse_lineups(data, ws_id, combo_id='2025-05-23ComInt')

    print(f'Players: {len(result["player"])}')
    for r in result['player'][:5]:
        cap = '(C)' if r['is_captain'] else '   '
        print(f'  {r["shirt_number"]:>2} {r["player_name"]:<25} {r["position"]:<4} {cap} team={r["team_name"]}')

    print(f'\nFormation changes: {len(result["formations"])}')
    for f in result['formations']:
        print(f'  {f["team_name"]:<12} [{f["start_minute"]}\'-{f["end_minute"]}\'] {f["formation_name"]}  captain={f["captain_name"]}')