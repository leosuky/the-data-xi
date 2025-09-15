import pandas as pd
import os


def game_summary(basepath: str, combo_id: str, match_id: int) -> pd.DataFrame:
    home_path = os.path.join(basepath, 'Home_Team_Summary.csv')
    away_path = os.path.join(basepath, 'Away_Team_Summary.csv')

    home_summary = pd.read_csv(home_path)
    away_summary = pd.read_csv(away_path)

    home_summary.iloc[-1, 0] = 'Home Team Total'
    away_summary.iloc[-1, 0] = 'Away Team Total'

    home_summary['is_home_team'] = True
    away_summary['is_home_team'] = False

    full_game_summary = pd.concat([home_summary, away_summary], ignore_index=True)
    full_game_summary['combo_id'] = combo_id
    full_game_summary['match_id'] = match_id

    return full_game_summary


def advanced_passing(basepath: str, combo_id: str, match_id: int) -> pd.DataFrame:
    home_path = os.path.join(basepath, 'Home_Team_Passing.csv')
    away_path = os.path.join(basepath, 'Away_Team_Passing.csv')

    home_passing = pd.read_csv(home_path)
    away_passing = pd.read_csv(away_path)

    home_passing.iloc[-1, 0] = 'Home Team Total'
    away_passing.iloc[-1, 0] = 'Away Team Total'

    home_passing['is_home_team'] = True
    away_passing['is_home_team'] = False

    full_game_passing = pd.concat([home_passing, away_passing], ignore_index=True)
    full_game_passing['combo_id'] = combo_id
    full_game_passing['match_id'] = match_id

    return full_game_passing


def adv_pass_types(basepath: str, combo_id: str, match_id: int) -> pd.DataFrame:
    home_path = os.path.join(basepath, 'Home_Team_Pass_Types.csv')
    away_path = os.path.join(basepath, 'Away_Team_Pass_Types.csv')

    home_pass_types = pd.read_csv(home_path)
    away_pass_types = pd.read_csv(away_path)

    home_pass_types.iloc[-1, 0] = 'Home Team Total'
    away_pass_types.iloc[-1, 0] = 'Away Team Total'

    home_pass_types['is_home_team'] = True
    away_pass_types['is_home_team'] = False

    full_game_pass_types = pd.concat([home_pass_types, away_pass_types], ignore_index=True)
    full_game_pass_types['combo_id'] = combo_id
    full_game_pass_types['match_id'] = match_id

    return full_game_pass_types


def advanced_defending(basepath: str, combo_id: str, match_id: int) -> pd.DataFrame:
    home_path = os.path.join(basepath, 'Home_Team_Defense.csv')
    away_path = os.path.join(basepath, 'Away_Team_Defense.csv')

    home_defence = pd.read_csv(home_path)
    away_defence = pd.read_csv(away_path)

    home_defence.iloc[-1, 0] = 'Home Team Total'
    away_defence.iloc[-1, 0] = 'Away Team Total'

    home_defence['is_home_team'] = True
    away_defence['is_home_team'] = False

    full_game_defence = pd.concat([home_defence, away_defence], ignore_index=True)
    full_game_defence['combo_id'] = combo_id
    full_game_defence['match_id'] = match_id

    return full_game_defence


def advanced_possession(basepath: str, combo_id: str, match_id: int) -> pd.DataFrame:
    home_path = os.path.join(basepath, 'Home_Team_Possession.csv')
    away_path = os.path.join(basepath, 'Away_Team_Possession.csv')

    home_possession = pd.read_csv(home_path)
    away_possession = pd.read_csv(away_path)

    home_possession.iloc[-1, 0] = 'Home Team Total'
    away_possession.iloc[-1, 0] = 'Away Team Total'

    home_possession['is_home_team'] = True
    away_possession['is_home_team'] = False

    full_game_possession = pd.concat([home_possession, away_possession], ignore_index=True)
    full_game_possession['combo_id'] = combo_id
    full_game_possession['match_id'] = match_id

    return full_game_possession


def misc_stats(basepath: str, combo_id: str, match_id: int) -> pd.DataFrame:
    home_path = os.path.join(basepath, 'Home_Team_Miscellaneous.csv')
    away_path = os.path.join(basepath, 'Away_Team_Miscellaneous.csv')

    home_misc = pd.read_csv(home_path)
    away_misc = pd.read_csv(away_path)

    home_misc.iloc[-1, 0] = 'Home Team Total'
    away_misc.iloc[-1, 0] = 'Away Team Total'

    home_misc['is_home_team'] = True
    away_misc['is_home_team'] = False

    full_game_misc = pd.concat([home_misc, away_misc], ignore_index=True)
    full_game_misc['combo_id'] = combo_id
    full_game_misc['match_id'] = match_id

    return full_game_misc


def gk_stats(basepath: str, combo_id: str, match_id: int) -> pd.DataFrame:
    home_path = os.path.join(basepath, 'Home_Team_Keeper.csv')
    away_path = os.path.join(basepath, 'Away_Team_Keeper.csv')

    home_gk = pd.read_csv(home_path)
    away_gk = pd.read_csv(away_path)

    home_gk['is_home_team'] = True
    away_gk['is_home_team'] = False

    full_game_gk = pd.concat([home_gk, away_gk], ignore_index=True)
    full_game_gk['combo_id'] = combo_id
    full_game_gk['match_id'] = match_id

    return full_game_gk

def shot_data(basepath: str, combo_id: str, match_id: int) -> pd.DataFrame:
    path = os.path.join(basepath, 'Shot_Data.csv')

    full = pd.read_csv(path)
    full = full.dropna(how='all')

    full['combo_id'] = combo_id
    full['match_id'] = match_id

    return full