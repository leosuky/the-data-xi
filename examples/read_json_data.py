import pandas as pd
import json
import re
from datetime import datetime, timedelta


def convert_timestamp_to_datetime(timestamp):
    if timestamp >=0:
        return datetime.fromtimestamp(timestamp)
    else:
        return datetime(1970, 1, 1) + timedelta(seconds=int(timestamp))

def flatten_dict(nested_dict, parent_key='', separator='_'):
    """
    Flattens a nested dictionary.

    Args:
        nested_dict (dict): The dictionary to flatten.
        parent_key (str): The base key for recursion.
        separator (str): The separator to join nested keys.

    Returns:
        dict: A flattened dictionary.
    """
    flat_dict = {}
    for key, value in nested_dict.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        
        # Check if the value is a dictionary and not empty
        if isinstance(value, dict) and value:
            # Recursively call the function for nested dictionaries
            flat_dict.update(flatten_dict(value, new_key, separator))
        else:
            flat_dict[new_key] = value
    return flat_dict


def players(filepath: str) -> dict:
    # Takes the lineups.json file
    with open(filepath, 'r', encoding='utf-8') as file:
        lineups = json.load(file)

    players_list = []
    home_formation = lineups['home']['formation']
    away_formation = lineups['away']['formation']

    for player in lineups['home']['players']:
        row = {}
        row['player_id'] = player['player']['id']
        row['name'] = player['player']['name']
        row['position'] = player['position']
        row['nationality'] = player['player']['country']['name']
        row['birth_date'] = datetime.fromtimestamp(player['player']['dateOfBirthTimestamp'])

        players_list.append(row)

    for player in lineups['away']['players']:
        row = {}
        row['player_id'] = player['player']['id']
        row['name'] = player['player']['name']
        row['position'] = player['position']
        row['nationality'] = player['player']['country']['name']
        row['birth_date'] = datetime.fromtimestamp(player['player']['dateOfBirthTimestamp'])

        players_list.append(row)

    players_df = pd.DataFrame(players_list)

    data = {
        'players': players_df,
        'home_formation': home_formation,
        'away_formation': away_formation
    }

    return data


def tournament_and_season(filepath: str) -> dict:
    # Takes the main_event.json file
    with open(filepath, 'r', encoding='utf-8') as file:
        event = json.load(file)

    Tournament = []

    row = {}
    row['tournament_id'] = event['event']['tournament']['uniqueTournament']['id']
    row['name'] = event['event']['tournament']['name']
    row['country'] = event['event']['tournament']['category']['country']['name']
    row['tier'] = event['event']['tournament']['competitionType']

    Tournament.append(row)
    Tournament_df = pd.DataFrame(Tournament)


    season = event['event']['season']
    season['tournament_id'] = Tournament[0]["tournament_id"] # Add the Tournament ID
    season_df = pd.DataFrame([season])

    data = {
        "tournament": Tournament_df,
        "season": season_df,
        "season_id": season["id"],
        "tournament_id": Tournament[0]["tournament_id"]
    }

    return data


def teams_and_referee(filepath: str) -> dict:
    with open(filepath, 'r', encoding='utf-8') as file:
        event = json.load(file)

    teams = []

    coords = event['event']['homeTeam']['venue'].get('venueCoordinates')
    row1 = {}
    row1['team_id'] = event['event']['homeTeam']['id']
    row1['name'] = event['event']['homeTeam']['name']
    row1['stadium'] = event['event']['homeTeam']['venue']['stadium']['name']
    row1['stadium_capacity'] = event['event']['homeTeam']['venue']['stadium']['capacity']
    row1['country'] = event['event']['homeTeam']['country']['name']
    row1['city'] = event['event']['homeTeam']['venue']['city']['name']
    row1['latitude'] = coords['latitude'] if coords else ''
    row1['longitude'] = coords['longitude'] if coords else ''
    row1['founded_date'] = convert_timestamp_to_datetime(
        event['event']['homeTeam']['foundationDateTimestamp']
    )

    teams.append(row1)

    coords = event['event']['awayTeam']['venue'].get('venueCoordinates')
    row2 = {}
    row2['team_id'] = event['event']['awayTeam']['id']
    row2['name'] = event['event']['awayTeam']['name']
    row2['stadium'] = event['event']['awayTeam']['venue']['stadium']['name']
    row2['stadium_capacity'] = event['event']['awayTeam']['venue']['stadium']['capacity']
    row2['country'] = event['event']['awayTeam']['country']['name']
    row2['city'] = event['event']['awayTeam']['venue']['city']['name']
    row2['latitude'] = coords['latitude'] if coords else ''
    row2['longitude'] = coords['longitude'] if coords else ''
    row2['founded_date'] = convert_timestamp_to_datetime(
        event['event']['awayTeam']['foundationDateTimestamp']
    )

    teams.append(row2)

    Teams_df = pd.DataFrame(teams)

    Referee = []

    row = {}
    row['name'] = event['event']['referee']['name']
    row['id'] = event['event']['referee']['id']
    row['nationality'] = event['event']['referee']['country']['name']
    row['red_cards'] = event['event']['referee']['redCards']
    row['yellow_cards'] = event['event']['referee']['yellowCards']
    row['double_yellow_cards'] = event['event']['referee']['yellowRedCards']
    row['games'] = event['event']['referee']['games']

    Referee.append(row)
    Referee_df = pd.DataFrame(Referee)

    data = {
        "teams": Teams_df,
        "referee": Referee_df,
        "home_team_id": row1['team_id'],
        "away_team_id": row2['team_id'],
        "referee_id": row['id']
    }

    return data


def managers(filepath: str) -> dict:
    with open(filepath, 'r', encoding='utf-8') as file:
        managers = json.load(file)

    managers['homeManager'].pop("fieldTranslations", None)
    managers['awayManager'].pop("fieldTranslations", None)

    managers_df = pd.DataFrame([
        managers['homeManager'],
        managers['awayManager']
    ])

    data = {
        "managers": managers_df,
        "home_manager_id": managers['homeManager']["id"],
        "away_manager_id": managers['awayManager']["id"]
    }

    return data


def match_details(
        filepath_event: str, filepath_best_players: str, home_manager: int, away_manager: int, 
        tournament: int, season: int, combo_id: str, home_formation: str, away_formation: str
) -> dict:
    with open(filepath_event, 'r', encoding='utf-8') as file:
        event = json.load(file)

    with open(filepath_best_players, 'r', encoding='utf-8') as file:
        best_players = json.load(file)

    match_id = event['event']['id']

    venue = event['event']['venue']['name']
    referee_id = event['event']['referee']['id']
    home_team_id = event['event']['homeTeam']['id']
    away_team_id = event['event']['awayTeam']['id']
    home_manager_id = home_manager
    away_manager_id = away_manager
    tournament_id = tournament
    season_id = season

    drop_columns = [
        'tournament', 'season', 'customId', 'status', 'venue', 'referee', 'homeTeam', 'awayTeam', 'changes', 
        'hasGlobalHighlights', 'hasXg', 'hasEventPlayerStatistics', 'hasEventPlayerHeatMap', 'detailId', 
        'crowdsourcingDataDisplayEnabled', 'awayRedCards', 'defaultPeriodCount', 'defaultPeriodLength', 
        'defaultOvertimeLength', 'varInProgress', 'slug', 'currentPeriodStartTimestamp', 'finalResultOnly', 'feedLocked',
        'fanRatingEvent', 'seasonStatisticsType', 'showTotoPromo', 'isEditor'
    ]

    match_data = {
        "tournament_id": tournament_id,
        "season_id": season_id,
        "combo_id": combo_id,
        "venue": venue,
        "referee_id": referee_id,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home_manager_id": home_manager_id,
        "away_manager_id": away_manager_id,
        "home_formation": home_formation,
        "away_formation": away_formation
    }

    match_data.update({k: v for k, v in event["event"].items() if k not in drop_columns})

    motm = {
        'motm_rating': best_players['playerOfTheMatch']['value'],
        'motm_player_name':best_players['playerOfTheMatch']['player']['name'],
        'motm_player_id':best_players['playerOfTheMatch']['player']['id']
    }

    match_data.update(motm)

    match_data = flatten_dict(match_data)

    match_data_df = pd.DataFrame([match_data])
    match_data_df.time_currentPeriodStartTimestamp = pd.to_datetime(match_data_df.time_currentPeriodStartTimestamp, unit='s')
    match_data_df.startTimestamp = pd.to_datetime(match_data_df.startTimestamp, unit='s')

    data = {
        "match": match_data_df,
        "match_id": match_id,
        "combo_id": combo_id
    }

    return data

def odds_table(filepath: str, combo_id: str) -> pd.DataFrame:
    with open(filepath, 'r', encoding='utf-8') as file:
        odds = json.load(file)

    match_id = odds['eventId']

    odds_dict = {}
    odds_dict["match_id"] = match_id
    odds_dict["combo_id"] = combo_id

    market_groups = ["1X2", "Double chance", "Both teams to score", "Match goals"]

    for i in odds['markets']:
        period = i.get('marketPeriod')
        group = i.get('marketGroup')
        choice = i.get('choiceGroup') + '_' if i.get('choiceGroup') else ''
        # winner = None
        if group in market_groups:
            for j in i['choices']:
                odds_dict[f"{period}_{group}_{choice}{j['name']}"] = j['fractionalValue']
            for j in i['choices']:
                if j.get('winning') == True:
                    odds_dict[f"{period}_{group}_{choice}winning"] = j['name']

    odds_df = pd.DataFrame([odds_dict])

    return odds_df


def shots_table(filepath: str, match_id: int, combo_id: str) -> pd.DataFrame:
    with open(filepath, 'r', encoding='utf-8') as file:
        shots = json.load(file)

    shots = shots['shotmap']

    shot_data = []
    for i in shots[::-1]:
        row = {}
        
        row['match_id'] = match_id
        row["combo_id"] = combo_id
        row['player_id'] = i['player']['id']
        row['player_name'] = i['player']['name']
        i.pop('player', None)
        row.update(i)
        
        shot_data.append(row)

    shot_df = pd.DataFrame(shot_data)

    return shot_df


def player_stats(filepath: str, match_id: int, combo_id: str, home_id: int, away_id: int) -> pd.DataFrame:
    # From the Lineups JSON file
    with open(filepath, 'r', encoding='utf-8') as file:
        lineups = json.load(file)

    home_team_players = [player['statistics'] for player in lineups['home']['players'] if player['statistics']]
    away_team_players = [player['statistics'] for player in lineups['away']['players'] if player['statistics']]

    player_stats = []

    for player in lineups['home']['players']:
        if player['statistics']:
            row = {}
            row['match_id'] = match_id
            row["combo_id"] = combo_id
            row['team_id'] = home_id
            row['is_home_team'] = True
            row['player_id'] = player['player']['id']
            row['position'] = player['position']
            rating = 'ratingVersions' in player['statistics']
            row['match_rating'] = player['statistics']['ratingVersions']['original'] if rating else 0
            row.update(player['statistics'])
            player_stats.append(row)

    for player in lineups['away']['players']:
        if player['statistics']:
            row = {}
            row['match_id'] = match_id
            row["combo_id"] = combo_id
            row['team_id'] = away_id
            row['is_home_team'] = False
            row['player_id'] = player['player']['id']
            row['position'] = player['position']
            rating = 'ratingVersions' in player['statistics']
            row['match_rating'] = player['statistics']['ratingVersions']['original'] if rating else 0
            row.update(player['statistics'])
            player_stats.append(row)

    player_stats_df = pd.DataFrame(player_stats)
    player_stats_df.fillna(0, inplace=True)

    return player_stats_df


def lineups_table(filepath: str, match_id: int, combo_id: str, home_id: int, away_id: int) -> pd.DataFrame:
    with open(filepath, 'r', encoding='utf-8') as file:
        lineups = json.load(file)

    lineup_data = []

    for i in range(len(lineups['home']['players'])):
        row = {}
        row['match_id'] = match_id
        row["combo_id"] = combo_id
        row['team_id'] = home_id
        row['is_home_team'] = True
        row['player_id'] = lineups['home']['players'][i]['player']['id']
        row['is_starter'] = not lineups['home']['players'][i]['substitute']
        minutes = 'minutesPlayed' in lineups['home']['players'][i]['statistics']
        captain = 'captain' in lineups['home']['players'][i]
        row['is_captain'] = captain
        row['minutes_played'] = lineups['home']['players'][i]['statistics']['minutesPlayed'] if minutes else 0
        row['shirt_number'] = lineups['home']['players'][i]['shirtNumber']
        row['position'] = lineups['home']['players'][i]['position']

        lineup_data.append(row)

    for i in range(len(lineups['away']['players'])):
        row = {}
        row['match_id'] = match_id
        row["combo_id"] = combo_id
        row['team_id'] = away_id
        row['is_home_team'] = False
        row['player_id'] = lineups['away']['players'][i]['player']['id']
        row['is_starter'] = not lineups['away']['players'][i]['substitute']
        minutes = 'minutesPlayed' in lineups['away']['players'][i]['statistics']
        captain = 'captain' in lineups['away']['players'][i]
        row['is_captain'] = captain
        row['minutes_played'] = lineups['away']['players'][i]['statistics']['minutesPlayed'] if minutes else 0
        row['shirt_number'] = lineups['away']['players'][i]['shirtNumber']
        row['position'] = lineups['away']['players'][i]['position']

        lineup_data.append(row)

    lineup_dframe = pd.DataFrame(lineup_data)

    return lineup_dframe


def match_stats(filepath: str, match_id: int, combo_id: str) -> pd.DataFrame:
    with open(filepath, 'r', encoding='utf-8') as file:
        statistics = json.load(file)

    stats = statistics['statistics']

    x_game_result = []

    for i, game_period in enumerate(stats):
        period = ['FULL-TIME', 'FIRST-HALF', 'SECOND-HALF'][i]
        row = {'period': period}
        row["match_id"] = match_id
        row["combo_id"] = combo_id

        for group in game_period['groups']:
            for stat in group['statisticsItems']:
                raw_name = stat.get("name", "").strip().lower()
                clean_name = re.sub(r'\W+', '_', raw_name)

                for key in ["homeValue", "awayValue", "homeTotal", "awayTotal"]:
                    if key in stat:
                        row[f"{clean_name}_{key.lower()}"] = stat[key]

        x_game_result.append(row)

    match_stats_df = pd.DataFrame(x_game_result)

    return match_stats_df

def misc_json_data(avg_positions: str, comments: str, graph: str, home_heatmap: str, away_heatmap: str, match_id: int, combo_id: str) -> pd.DataFrame:
    with open(avg_positions, 'r', encoding='utf-8') as file:
        avg_positions = json.load(file)
    with open(comments, 'r', encoding='utf-8') as file:
        comments = json.load(file)
    with open(graph, 'r', encoding='utf-8') as file:
        graph = json.load(file)
    with open(home_heatmap, 'r', encoding='utf-8') as file:
        home_heatmap = json.load(file)
    with open(away_heatmap, 'r', encoding='utf-8') as file:
        away_heatmap = json.load(file)

    data = [{
        "match_id": match_id,
        "combo_id": combo_id,
        "average_positions": avg_positions,
        "commentary": comments,
        "match_momentum_graph": graph,
        "home_heatmap": home_heatmap,
        "away_heatmap": away_heatmap
    }]

    data_df = pd.DataFrame(data)

    return data_df