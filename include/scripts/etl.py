import os
import pandas as pd
import pathlib
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from include.helpers import read_csv_data as rdcsv
from include.helpers import read_json_data as rdjson

# Airflow specific import to fetch the connection
from airflow.sdk import Connection

ENV_PATH = os.getcwd()
DATA_PATH = os.path.join(ENV_PATH, 'data/')

def find_json_files(directory):
    
    """
    Finds and returns the full paths of all JSON files in a given directory and its subdirectories.

    Args:
        directory (str): The starting directory to search.

    Returns:
        list: A list of pathlib.Path objects for all found JSON files.
    """
    json_files = []
    # Use pathlib to create a Path object for the starting directory
    base_path = pathlib.Path(directory)

    # Check if the directory exists to avoid errors
    if not base_path.is_dir():
        print(f"Error: Directory not found at {directory}")
        return json_files

    for dirpath, dirnames, filenames in os.walk(base_path):
        # dirpath: a string of the path to the current directory
        # dirnames: a list of the names of the subdirectories in dirpath
        # filenames: a list of the names of the files in dirpath
        for filename in filenames:
            if filename.endswith('.json'):
                full_path = pathlib.Path(dirpath) / filename
                json_files.append(full_path)

    return json_files


def store_dataframe_to_postgres(df, table_name, conn):
    """
    Stores a pandas DataFrame to a PostgreSQL table, automatically
    mapping dictionary columns to JSONB.
    """
    # 1. Create a dictionary to hold the custom column types
    dtype_dict = {}

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


def process_data(csv_dir: str, json_files: list, combo_id: str) -> dict[pd.DataFrame]:

    if len(json_files) < int(13):
        raise("The Number of Json Files is LESS THAN 13, Kindly address")

    # Get the names of all json files
    file_names = {}
    for path in json_files:
        str_path = str(path)
        str_path = str_path.split("\\")[-1]
        str_path = str_path.split("_", 1)[-1].replace(".json", "")
        if "heatmap_away_team" in str_path:
            str_path = "heatmap_away_team"
        if "heatmap_home_team" in str_path:
            str_path = "heatmap_home_team"

        file_names[str_path] = path

    try:
        print("Parsing json and csv files...")
        # READ THE JSON FILES ========================================================>
        player_data = rdjson.players(file_names["lineups"]) # players, home_formation, away_formation

        tournament_data = rdjson.tournament_and_season(file_names["main_event_data"]) # tournament, season, season_id, tournament_id

        teams_and_ref = rdjson.teams_and_referee(file_names["main_event_data"]) # teams, referee, home_team_id, away_team_id, referee_id

        managers = rdjson.managers(file_names["managers"]) # managers, home_manager_id, away_manager_id

        match_details = rdjson.match_details(
            file_names["main_event_data"], file_names["best_players_summary"], managers["home_manager_id"], managers["away_manager_id"],
            tournament_data["tournament_id"], tournament_data["season_id"], combo_id, 
            player_data["home_formation"], player_data["away_formation"]
        ) # match, match_id, combo_id

        odds_data = rdjson.odds_table(file_names["odds_all"], match_details["combo_id"])

        shots = rdjson.shots_table(file_names["shotmap"], match_details["match_id"], match_details["combo_id"])

        player_stats = rdjson.player_stats(
            file_names["lineups"], match_details["match_id"], match_details["combo_id"],
            teams_and_ref["home_team_id"], teams_and_ref["away_team_id"]
        )

        lineup = rdjson.lineups_table(
            file_names["lineups"], match_details["match_id"], match_details["combo_id"], 
            teams_and_ref["home_team_id"], teams_and_ref["away_team_id"]
        )

        match_stats = rdjson.match_stats(file_names["statistics"], match_details["match_id"], match_details["combo_id"])

        miscellaneous_data = rdjson.misc_json_data(
            file_names["average_positions"], file_names["comments"], file_names["graph"],
            file_names["heatmap_home_team"], file_names["heatmap_away_team"], match_details["match_id"],
            match_details["combo_id"]
        )
        # ================== ========================================================>

        # READ THE CSV FILES ========================================================>
        game_summary = rdcsv.game_summary(csv_dir, match_details["combo_id"], match_details["match_id"])
        adv_pass_types = rdcsv.adv_pass_types(csv_dir, match_details["combo_id"], match_details["match_id"])
        advanced_passing = rdcsv.advanced_passing(csv_dir, match_details["combo_id"], match_details["match_id"])
        advanced_defending = rdcsv.advanced_defending(csv_dir, match_details["combo_id"], match_details["match_id"])
        advanced_possession = rdcsv.advanced_possession(csv_dir, match_details["combo_id"], match_details["match_id"])
        misc_stats = rdcsv.misc_stats(csv_dir, match_details["combo_id"], match_details["match_id"])
        gk_stats = rdcsv.gk_stats(csv_dir, match_details["combo_id"], match_details["match_id"])
        shot_data = rdcsv.shot_data(csv_dir, match_details["combo_id"], match_details["match_id"])
        # ================== ========================================================>
        
        print("All json and csv files have been successfully parsed")

    except Exception as error:
        raise(f"An Error has occured: {error}")

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
        "advanced_shot_data": shot_data,
        "match_stats": match_stats,
        "lineup": lineup,
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


def run_etl(start_directory: str):

    # Create connection from Airflow to Postgres
    # conn = BaseHook.get_connection('football_db')
    # db_uri = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    # engine = create_engine(db_uri)
    conn = Connection.get("the_data_xi_postgres")

    uri = conn.get_uri()
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(uri)
    try:
        conn = engine.connect()
        print(conn)
        print("Successfully connected to PostgreSQL via Airflow connection.")
    except Exception as e:
        raise(e)

    # Walk through our directories
    for root, dirs, files in os.walk(start_directory):
        # If there are any files found in a directory
        if files:
            # Get our csv and json files.
            directory_to_csv = root
            json_files = find_json_files(root)
            # Retrieve the combo_id
            combo_id = root.split('/')[-1]

            # Process Data
            print("Now Processing Data...\n")
            all_data = process_data(directory_to_csv, json_files, combo_id)
            print("Data Processing Complete")

            print("Pushing files to Postgres")
            for table_name, dataframe in all_data.items():
                try:
                    store_dataframe_to_postgres(dataframe, table_name, conn)
                except Exception as err:
                    raise(f"Error storing table '{table_name}' for match {combo_id}: {err}")

            print(f"Successfully pushed data to postgres for match {combo_id}")

    print("ETL process finished successfully.")

            
