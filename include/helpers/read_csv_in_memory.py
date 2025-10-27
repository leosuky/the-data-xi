import pandas as pd
import numpy as np

def combine_dfs(home_path: pd.DataFrame, away_path: pd.DataFrame, combo_id: str, match_id: int) -> pd.DataFrame:

    home_path.iloc[-1, 0] = 'Home Team Total'
    away_path.iloc[-1, 0] = 'Away Team Total'

    home_path['is_home_team'] = True
    away_path['is_home_team'] = False

    full_game = pd.concat([home_path, away_path], ignore_index=True)
    full_game['combo_id'] = combo_id
    full_game['match_id'] = match_id
    
    return full_game

def combine_gk(home_path: pd.DataFrame, away_path: pd.DataFrame, combo_id: str, match_id: int) -> pd.DataFrame:

    home_path['is_home_team'] = True
    away_path['is_home_team'] = False

    full_game = pd.concat([home_path, away_path], ignore_index=True)
    full_game['combo_id'] = combo_id
    full_game['match_id'] = match_id
    
    return full_game


def shot_data(path: pd.DataFrame, combo_id: str, match_id: int) -> pd.DataFrame:
    
    full = path
    
    full = full.dropna(subset=["Minute"])

    full['combo_id'] = combo_id
    full['match_id'] = match_id

    full['row_id'] = full.index

    return full