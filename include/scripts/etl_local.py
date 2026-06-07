import os, io, json, csv
import numpy as np
import pandas as pd
import pathlib, collections
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from include.helpers import read_csv_data as rd_csv, read_json_data as rd_json
from airflow.sdk import Connection
import logging

# LOGGER
log = logging.getLogger(__name__)

def parse_file_name(str_path: str) -> str:
    
    if str_path.endswith(".json"):
        str_path = str_path.split("_", 1)[-1].replace(".json", "")
        if "heatmap_away_team" in str_path:
            str_path = "heatmap_away_team"
        if "heatmap_home_team" in str_path:
            str_path = "heatmap_home_team"

        return str_path
    else:
        return str_path

# Get all filenames from directory.
def get_file_names_from_local_dir(base_dir: str) -> list[dict]:
    """
    Scans a local directory (e.g., '/usr/local/airflow/data') for match data
    and returns a list of items for the Airflow DAG to map over.
    
    Assumes a structure like: <base_dir>/<League>/<Season>/<GW>/<combo_id>/[files]
    """
    log.info(f"Scanning for match folders in local directory: {base_dir}")
    tree = collections.defaultdict(list)
    
    for root, dirs, files in os.walk(base_dir):
        if not dirs: # This means we are in a leaf directory (e.g., a combo_id folder)
            # We want the 'prefix' to be the path to the combo_id folder
            # and the 'combo_id' to be the folder name itself.
            path = root
            combo_id = os.path.basename(root)
            
            if len(files) < 28:
                log.warning(f"Skipping path {path}: Found {len(files)} files, expected 28.")
                continue
                
            # Store the file list for this path
            tree[path] = files

    # Prepare the map_list for Airflow, same as the OCI version
    map_list = []
    for path, files in tree.items():
        combo_id = os.path.basename(path)
        map_list.append({"combo_id": combo_id, "path": path, "files": files})

    if not map_list:
        log.warning("No valid match folders found in local directory.")
        
    log.info(f"Found {len(map_list)} valid match folders to process.")
    return map_list


def connect_to_postgres():
    conn = Connection.get("the_data_xi_postgres")

    uri = conn.get_uri()
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(uri, isolation_level="AUTOCOMMIT")
    try:
        conn = engine.connect()
        print(conn)
        print("Successfully connected to PostgreSQL via Airflow connection.")
        return conn
    except Exception as e:
        raise(e)
    
def ensure_schema_match_raw_table(df, table_name, conn):
    """
    Checks the database schema against the DataFrame schema and adds missing columns (DDL).
    """
    connection = conn
    try:
        # create the table if it doesn't exist
        connection.execute(f"CREATE TABLE IF NOT EXISTS raw.{table_name} ()")

        existing_cols = [
            row[0] for row in connection.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = '{table_name}'"
            )
        ]

        # Make column names lower case
        df.columns = df.columns.str.lower()
        df_cols = df.columns.values.tolist()

        # set difference of columns
        cols_diff = list(set(df_cols) - set(existing_cols))

        # Check if the columns match
        if len(cols_diff) == 0:
            print("Same Exact Columns! All good")
            return
        
        # 2. Iterate through DataFrame columns and add missing ones
        # for col_name, dtype in df.dtypes.items():
        #     col_name_lower = col_name.lower() # Already lowercased by the calling function, but safe to check

        for col_name in cols_diff:
            dtype = df[col_name].dtype
            col_name_lower = col_name.lower()
            
            if col_name_lower not in existing_cols:
                # Determine the SQL type. Use TEXT as a safe default for JSON-heavy data.
                sql_type = 'TEXT'
                
                if 'bool' in str(dtype): # Check for pandas boolean type
                    sql_type = 'BOOLEAN'
                elif dtype == 'object':
                        # Check for dictionary/list objects to map to JSONB/TEXT
                    first_val = df[col_name].dropna().iloc[0] if not df[col_name].isnull().all() else None
                    if isinstance(first_val, (dict, list)):
                        sql_type = 'JSONB'
                    
                elif 'int' in str(dtype):
                    sql_type = 'BIGINT'
                elif 'float' in str(dtype):
                    sql_type = 'DECIMAL'
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = 'TIMESTAMPTZ'

                # Execute DDL to add the new column
                print(f"Schema Evolution: Adding column raw.{table_name}.{col_name_lower} as {sql_type}")
                connection.execute(text(f'ALTER TABLE raw.{table_name} ADD COLUMN IF NOT EXISTS"{col_name_lower}" {sql_type};'))

    except Exception as e:
        raise(f"Error during schema check/evolution for {table_name}: {e}")
        # Re-raise the error to fail the task if DDL fails

def store_dataframe_to_postgres(df, table_name, conn):
    """
    Stores a pandas DataFrame to a PostgreSQL table, automatically
    mapping dictionary columns to JSONB.
    """
    # 1. Create a dictionary to hold the custom column types
    dtype_dict = {}

    # Turn any empty strings to null
    df = df.replace(r'^\s*$', np.nan, regex=True)

    # Make column names lower case
    df.columns = df.columns.str.lower()

    # 2. Iterate through each column to check its data type
    for col_name, dtype in df.dtypes.items():
        # Check if the column's data type is 'object'
        if dtype == 'object':
            # 3. Check if the first non-null value in the column is a dictionary
            first_val = df[col_name].dropna().iloc[0] if not df[col_name].isnull().all() else None
            
            if isinstance(first_val, dict):
                print(f"Detected JSON data in column '{col_name}'. Mapping to JSONB.")
                dtype_dict[col_name] = JSONB
    
    # 4. Use the custom dtype dictionary when writing to SQL
    df.to_sql(
        name=table_name,
        con=conn,
        if_exists='append',
        index=False,
        schema='raw',
        dtype=dtype_dict
    )
    print(f"DataFrame successfully stored in table '{table_name}'.")

# Process All Data for a single Game.
def process_data(match: dict) -> dict[pd.DataFrame]:

    combo_id = match['combo_id']
    path_to_files = match['path']
    files = match['files']

    files_dict = {}
    for file in files:
        parsed_name = parse_file_name(file)
        file_path = os.path.join(path_to_files, file)
        files_dict[parsed_name] = file_path

    # PROCESS FILES
    log.info(f"Now Processing files for Match: {combo_id}")
    try:
        print("Parsing json and csv files...")
        # READ THE JSON FILES ========================================================>
        player_data = rd_json.players(files_dict["lineups"]) # players, home_formation, away_formation

        tournament_data = rd_json.tournament_and_season(files_dict["main_event_data"]) # tournament, season, season_id, tournament_id

        teams_and_ref = rd_json.teams_and_referee(files_dict["main_event_data"]) # teams, referee, home_team_id, away_team_id, referee_id

        managers = rd_json.managers(files_dict["managers"]) # managers, home_manager_id, away_manager_id

        match_details = rd_json.match_details(
            files_dict["main_event_data"], files_dict["best_players_summary"], managers["home_manager_id"], managers["away_manager_id"],
            tournament_data["tournament_id"], tournament_data["season_id"], combo_id, 
            player_data["home_formation"], player_data["away_formation"]
        ) # match, match_id, combo_id

        odds_data = rd_json.odds_table(files_dict["odds_all"], match_details["combo_id"], match_details["match_id"])

        shots = rd_json.shots_table(files_dict["shotmap"], match_details["match_id"], match_details["combo_id"])

        player_stats = rd_json.player_stats(
            files_dict["lineups"], match_details["match_id"], match_details["combo_id"],
            teams_and_ref["home_team_id"], teams_and_ref["away_team_id"]
        )

        lineup = rd_json.lineups_table(
            files_dict["lineups"], match_details["match_id"], match_details["combo_id"], 
            teams_and_ref["home_team_id"], teams_and_ref["away_team_id"]
        )

        missing_players = rd_json.missing_players(
            files_dict["lineups"], match_details["match_id"], match_details["combo_id"], 
            teams_and_ref["home_team_id"], teams_and_ref["away_team_id"]
        )

        match_stats = rd_json.match_stats(files_dict["statistics"], match_details["match_id"], match_details["combo_id"])

        miscellaneous_data = rd_json.misc_json_data(
            files_dict["average_positions"], files_dict["comments"], files_dict["graph"],
            files_dict["heatmap_home_team"], files_dict["heatmap_away_team"], match_details["match_id"],
            match_details["combo_id"], full_heatmaps=files_dict.get('heatmaps', {'None': None})
        )
        # ================== ========================================================>

        # READ THE CSV FILES ========================================================>
        game_summary = rd_csv.combine_dfs(
            files_dict['Home_Team_Summary.csv'],files_dict['Away_Team_Summary.csv'], 
            match_details["combo_id"], match_details["match_id"]
        )
        adv_pass_types = rd_csv.combine_dfs(
            files_dict["Home_Team_Pass_Types.csv"], files_dict["Away_Team_Pass_Types.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        advanced_passing = rd_csv.combine_dfs(
            files_dict["Home_Team_Passing.csv"], files_dict["Away_Team_Passing.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        advanced_defending = rd_csv.combine_dfs(
            files_dict["Home_Team_Defense.csv"], files_dict["Away_Team_Defense.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        advanced_possession = rd_csv.combine_dfs(
            files_dict["Home_Team_Possession.csv"], files_dict["Away_Team_Possession.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        misc_stats = rd_csv.combine_dfs(
            files_dict["Home_Team_Miscellaneous.csv"], files_dict["Away_Team_Miscellaneous.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        gk_stats = rd_csv.combine_gk(
            files_dict["Home_Team_Keeper.csv"], files_dict["Away_Team_Keeper.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        shot_data = rd_csv.shot_data(
            files_dict["Shot_Data.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        # ================== ========================================================>
        
        print("All json and csv files have been successfully parsed")

    except Exception as error:
        raise(f"An Error has occured for {combo_id}: {error}")
    
    # Consolidate all dataframes into one dictionary
    all_data = {
        "game_summary": game_summary,
        "advanced_pass_types": adv_pass_types,
        "advanced_passing": advanced_passing,
        "advanced_defending": advanced_defending,
        "advanced_possession": advanced_possession,
        "misc_stats": misc_stats,
        "gk_stats": gk_stats,
        "shots": shots,
        "tournaments": tournament_data["tournament"],
        "seasons": tournament_data["season"],
        "advanced_shot_data": shot_data,
        "match_stats": match_stats,
        "lineup": lineup,
        "missing_players": missing_players,
        "player_stats": player_stats,
        "odds_data": odds_data,
        "match_details": match_details["match"],
        "managers": managers["managers"],
        "referee": teams_and_ref["referee"],
        "teams": teams_and_ref["teams"],
        "player_data": player_data["players"],
        "misc_json_data": miscellaneous_data
    }

    return all_data

