import pandas as pd


# Apparently loading files into memory turns numbers into strings
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Replace empty strings with NaN
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    # Finally, replace NaN with None so Postgres gets NULLs
    df = df.where(pd.notnull(df), None)

    return df


def game_summary(home_path: list, away_path: list, combo_id: str, match_id: int) -> pd.DataFrame:

    home_summary = pd.DataFrame(home_path).astype('int64', errors='ignore')
    away_summary = pd.DataFrame(away_path).astype('int64', errors='ignore')

    home_summary.iloc[-1, 0] = 'Home Team Total'
    away_summary.iloc[-1, 0] = 'Away Team Total'

    home_summary['is_home_team'] = True
    away_summary['is_home_team'] = False

    full_game_summary = pd.concat([home_summary, away_summary], ignore_index=True)
    full_game_summary['combo_id'] = combo_id
    full_game_summary['match_id'] = match_id
    full_game_summary = clean_dataframe(full_game_summary)

    return full_game_summary


def advanced_passing(home_path: list, away_path: list, combo_id: str, match_id: int) -> pd.DataFrame:
    
    home_passing = pd.DataFrame(home_path).astype('int64', errors='ignore')
    away_passing = pd.DataFrame(away_path).astype('int64', errors='ignore')

    home_passing.iloc[-1, 0] = 'Home Team Total'
    away_passing.iloc[-1, 0] = 'Away Team Total'

    home_passing['is_home_team'] = True
    away_passing['is_home_team'] = False

    full_game_passing = pd.concat([home_passing, away_passing], ignore_index=True)
    full_game_passing['combo_id'] = combo_id
    full_game_passing['match_id'] = match_id
    full_game_passing = clean_dataframe(full_game_passing)
    
    return full_game_passing


def adv_pass_types(home_path: list, away_path: list, combo_id: str, match_id: int) -> pd.DataFrame:
    
    home_pass_types = pd.DataFrame(home_path).astype('int64', errors='ignore')
    away_pass_types = pd.DataFrame(away_path).astype('int64', errors='ignore')

    home_pass_types.iloc[-1, 0] = 'Home Team Total'
    away_pass_types.iloc[-1, 0] = 'Away Team Total'

    home_pass_types['is_home_team'] = True
    away_pass_types['is_home_team'] = False

    full_game_pass_types = pd.concat([home_pass_types, away_pass_types], ignore_index=True)
    full_game_pass_types['combo_id'] = combo_id
    full_game_pass_types['match_id'] = match_id
    full_game_pass_types = clean_dataframe(full_game_pass_types)

    return full_game_pass_types


def advanced_defending(home_path: list, away_path: list, combo_id: str, match_id: int) -> pd.DataFrame:
    
    home_defence = pd.DataFrame(home_path).astype('int64', errors='ignore')
    away_defence = pd.DataFrame(away_path).astype('int64', errors='ignore')

    home_defence.iloc[-1, 0] = 'Home Team Total'
    away_defence.iloc[-1, 0] = 'Away Team Total'

    home_defence['is_home_team'] = True
    away_defence['is_home_team'] = False

    full_game_defence = pd.concat([home_defence, away_defence], ignore_index=True)
    full_game_defence['combo_id'] = combo_id
    full_game_defence['match_id'] = match_id
    full_game_defence = clean_dataframe(full_game_defence)

    return full_game_defence


def advanced_possession(home_path: list, away_path: list, combo_id: str, match_id: int) -> pd.DataFrame:
    
    home_possession = pd.DataFrame(home_path).astype('int64', errors='ignore')
    away_possession = pd.DataFrame(away_path).astype('int64', errors='ignore')

    home_possession.iloc[-1, 0] = 'Home Team Total'
    away_possession.iloc[-1, 0] = 'Away Team Total'

    home_possession['is_home_team'] = True
    away_possession['is_home_team'] = False

    full_game_possession = pd.concat([home_possession, away_possession], ignore_index=True)
    full_game_possession['combo_id'] = combo_id
    full_game_possession['match_id'] = match_id
    full_game_possession = clean_dataframe(full_game_possession)

    return full_game_possession


def misc_stats(home_path: list, away_path: list, combo_id: str, match_id: int) -> pd.DataFrame:
    
    home_misc = pd.DataFrame(home_path).astype('int64', errors='ignore')
    away_misc = pd.DataFrame(away_path).astype('int64', errors='ignore')

    home_misc.iloc[-1, 0] = 'Home Team Total'
    away_misc.iloc[-1, 0] = 'Away Team Total'

    home_misc['is_home_team'] = True
    away_misc['is_home_team'] = False

    full_game_misc = pd.concat([home_misc, away_misc], ignore_index=True)
    full_game_misc['combo_id'] = combo_id
    full_game_misc['match_id'] = match_id
    full_game_misc = clean_dataframe(full_game_misc)

    return full_game_misc


def gk_stats(home_path: list, away_path: list, combo_id: str, match_id: int) -> pd.DataFrame:
    
    home_gk = pd.DataFrame(home_path).astype('int64', errors='ignore')
    away_gk = pd.DataFrame(away_path).astype('int64', errors='ignore')

    home_gk['is_home_team'] = True
    away_gk['is_home_team'] = False

    full_game_gk = pd.concat([home_gk, away_gk], ignore_index=True)
    full_game_gk['combo_id'] = combo_id
    full_game_gk['match_id'] = match_id
    full_game_gk = clean_dataframe(full_game_gk)

    return full_game_gk

def shot_data(path: list, combo_id: str, match_id: int) -> pd.DataFrame:
    
    full = pd.DataFrame(path).astype('int64', errors='ignore')

    full = clean_dataframe(full)
    full = full.dropna(subset=["Minute"])

    full['combo_id'] = combo_id
    full['match_id'] = match_id

    return full