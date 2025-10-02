import os, io, json, csv, oci
import pandas as pd
import pathlib, collections
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from include.helpers import read_csv_in_memory as rdmcsv
from include.helpers import read_json_in_memory as rdmjson
from airflow.sdk import Connection
from airflow.providers.standard.operators.bash import BashOperator



CONFIG = oci.config.from_file(file_location="/usr/local/airflow/.oci/config")
OBJECT_STORAGE = oci.object_storage.ObjectStorageClient(CONFIG)
NAMESPACE = OBJECT_STORAGE.get_namespace().data
BUCKET_NAME = "the_data_xi"
BASE_PREFIX = 'Done/'


# Convert flat folder structure to nested.
def build_tree(object_list, base_prefix="Done/"):
    tree = collections.defaultdict(list)
    for obj in object_list:
        # Remove base prefix
        if "." in obj.name:
            relative_path = obj.name[len(base_prefix):]
            parts = relative_path.split("/")
            if len(parts) > 1:
                tree["/".join(parts[:-1])].append(parts[-1])
    return tree

def get_file_names_from_bucket():
    # List everything under Done/
    objects = OBJECT_STORAGE.list_objects(
        namespace_name=NAMESPACE,
        bucket_name=BUCKET_NAME,
        prefix=BASE_PREFIX
    )

    # Build a pseudo-folder structure
    tree = build_tree(objects.data.objects, base_prefix=BASE_PREFIX)
    # A dict where k = path/combo_id, v = [filenames]

    return tree


def process_object_in_memory(bucket_name: str, object_name: str) -> dict | list:
    """
    Download an object from OCI Object Storage and process it
    depending on its extension (.json or .csv).
    """

    # Fetch object
    response = OBJECT_STORAGE.get_object(NAMESPACE, bucket_name, object_name)
    content = response.data.content.decode("utf-8")

    if object_name.endswith(".json"):
        # Parse JSON
        data = json.loads(content)
        print(f"Processed JSON file {object_name} → {len(data)} records")
        return data # ---> return {dictionary}

    elif object_name.endswith(".csv"):
        # Parse CSV using csv.DictReader (maps header → values)
        reader = csv.DictReader(io.StringIO(content))
        rows = [row for row in reader]
        print(f"Processed CSV file {object_name} → {len(rows)} rows")
        return rows # ----> returns [List of Dictionaries]

    else:
        print(f"Skipping unsupported file type: {object_name}")
        return None
    

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


def download_objects_to_memory(prefix: str, files: list):

    all_files_in_memory = {}

    for file in files:
        object_name = f"{BASE_PREFIX}{prefix}/{file}"
        object_file = process_object_in_memory(BUCKET_NAME, object_name)

        # Save file in dictionary
        data_name = parse_file_name(file)

        all_files_in_memory[data_name] = object_file

    return all_files_in_memory

def connect_to_postgres():
    conn = Connection.get("the_data_xi_postgres")

    uri = conn.get_uri()
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(uri)
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
    This function must run BEFORE df.to_sql(if_exists='append').
    """
    try:
        # 1. Get existing column names from PostgreSQL information schema
        with conn as connection:
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
                    
                    if dtype == 'object':
                         # Check for dictionary/list objects to map to JSONB/TEXT
                        first_val = df[col_name].dropna().iloc[0] if not df[col_name].isnull().all() else None
                        if isinstance(first_val, (dict, list)):
                            sql_type = 'JSONB'
                        
                    elif 'int' in str(dtype):
                        sql_type = 'BIGINT'
                    elif 'float' in str(dtype):
                        sql_type = 'DECIMAL'

                    # Execute DDL to add the new column
                    print(f"Schema Evolution: Adding column raw.{table_name}.{col_name_lower} as {sql_type}")
                    connection.execute(f"ALTER TABLE raw.{table_name} ADD COLUMN {col_name_lower} {sql_type};")

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

def process_data(combo_id: str, all_files_in_memory: dict) -> pd.DataFrame:

    try:
        print("Parsing json and csv files...")
        # READ THE JSON FILES ========================================================>
        player_data = rdmjson.players(all_files_in_memory["lineups"]) # players, home_formation, away_formation

        tournament_data = rdmjson.tournament_and_season(all_files_in_memory["main_event_data"]) # tournament, season, season_id, tournament_id

        teams_and_ref = rdmjson.teams_and_referee(all_files_in_memory["main_event_data"]) # teams, referee, home_team_id, away_team_id, referee_id

        managers = rdmjson.managers(all_files_in_memory["managers"]) # managers, home_manager_id, away_manager_id

        match_details = rdmjson.match_details(
            all_files_in_memory["main_event_data"], all_files_in_memory["best_players_summary"], managers["home_manager_id"], managers["away_manager_id"],
            tournament_data["tournament_id"], tournament_data["season_id"], combo_id, 
            player_data["home_formation"], player_data["away_formation"]
        ) # match, match_id, combo_id

        odds_data = rdmjson.odds_table(all_files_in_memory["odds_all"], match_details["combo_id"])

        shots = rdmjson.shots_table(all_files_in_memory["shotmap"], match_details["match_id"], match_details["combo_id"])

        player_stats = rdmjson.player_stats(
            all_files_in_memory["lineups"], match_details["match_id"], match_details["combo_id"],
            teams_and_ref["home_team_id"], teams_and_ref["away_team_id"]
        )

        lineup = rdmjson.lineups_table(
            all_files_in_memory["lineups"], match_details["match_id"], match_details["combo_id"], 
            teams_and_ref["home_team_id"], teams_and_ref["away_team_id"]
        )

        match_stats = rdmjson.match_stats(all_files_in_memory["statistics"], match_details["match_id"], match_details["combo_id"])

        miscellaneous_data = rdmjson.misc_json_data(
            all_files_in_memory["average_positions"], all_files_in_memory["comments"], all_files_in_memory["graph"],
            all_files_in_memory["heatmap_home_team"], all_files_in_memory["heatmap_away_team"], match_details["match_id"],
            match_details["combo_id"]
        )
        # ================== ========================================================>

        # READ THE CSV FILES ========================================================>
        game_summary = rdmcsv.game_summary(
            all_files_in_memory['Home_Team_Summary.csv'],all_files_in_memory['Away_Team_Summary.csv'], 
            match_details["combo_id"], match_details["match_id"]
        )
        adv_pass_types = rdmcsv.adv_pass_types(
            all_files_in_memory["Home_Team_Passing.csv"], all_files_in_memory["Home_Team_Passing.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        advanced_passing = rdmcsv.advanced_passing(
            all_files_in_memory["Home_Team_Pass_Types.csv"], all_files_in_memory["Home_Team_Pass_Types.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        advanced_defending = rdmcsv.advanced_defending(
            all_files_in_memory["Home_Team_Defense.csv"], all_files_in_memory["Home_Team_Defense.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        advanced_possession = rdmcsv.advanced_possession(
            all_files_in_memory["Home_Team_Possession.csv"], all_files_in_memory["Home_Team_Possession.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        misc_stats = rdmcsv.misc_stats(
            all_files_in_memory["Home_Team_Miscellaneous.csv"], all_files_in_memory["Away_Team_Miscellaneous.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        gk_stats = rdmcsv.gk_stats(
            all_files_in_memory["Home_Team_Keeper.csv"], all_files_in_memory["Away_Team_Keeper.csv"],
            match_details["combo_id"], match_details["match_id"]
        )
        shot_data = rdmcsv.shot_data(
            all_files_in_memory["Shot_Data.csv"],
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
        "seaasons": tournament_data["season"],
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


test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command="cd /usr/local/airflow/the_data_xi_dbt && dbt run --models staging"
    )

def run_etl_oci():
    # Get all the files in our bucket in a nested folder structure
    print("Retrieving file names from OCI Bucket")
    nested_files = get_file_names_from_bucket()
    print("Retrieval Successful!")

    # Establish Connection to Postgres
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

    for prefix, files in nested_files.items():
        if len(files) < 28:
            raise(f"Error!! The total files are less than 28 for Folder: {prefix}")
        else:
            # Get all the files in memory
            combo_id = prefix.split('/')[-1]
            print("Downoading objects from OCI Bucket to memory.....")
            all_files_in_memory = download_objects_to_memory(prefix, files)
            print("Download Complete!")

            # Process Data
            print("Now Processing Data...\n")
            all_data = process_data(combo_id, all_files_in_memory)
            print("Data Processing Complete")

            print("Pushing files to Postgres")
            for table_name, dataframe in all_data.items():
                try:
                    store_dataframe_to_postgres(dataframe, table_name, conn)
                except Exception as err:
                    raise(f"Error storing table '{table_name}' for match {combo_id}: {err}")

            print(f"Successfully pushed data to postgres for match {combo_id}")

            print("Pushing Data to staging schema")
            test_dbt()
            print('Done')

