# pipelines/scripts/extract_and_load.py

import os
import pandas as pd
import pathlib
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from include.helpers import read_csv_data as rdcsv
from include.helpers import read_json_data as rdjson

# Airflow specific import to fetch the connection
from airflow.hooks.base import BaseHook

# These imports would be your actual data processing modules
# For this example, we'll assume their logic is contained here or they are available in the environment.
# import read_json_data as rdjson
# import read_csv_data as rdcsv

# --- MOCK DATA PROCESSING FUNCTIONS (Replace with your actual imports) ---
# To make this script runnable for demonstration, I'm creating placeholder functions.
# In your real setup, you would just import your 'rdjson' and 'rdcsv' modules.
def mock_processing(file_path, combo_id, match_id):
    """Placeholder for your complex data reading and transformation logic."""
    print(f"Processing {file_path} for match {match_id}")
    # In reality, this would return a structured pandas DataFrame.
    # We return a simple one for demonstration.
    return pd.DataFrame([{'match_id': match_id, 'combo_id': combo_id, 'data': 'some_data'}])

# --- END MOCK FUNCTIONS ---

def store_dataframe_to_postgres(df, table_name, engine):
    """
    Stores a pandas DataFrame to a PostgreSQL table, automatically
    mapping dictionary columns to JSONB.
    MODIFIED: Changed to 'append' for the ETL strategy.
    """
    dtype_dict = {}
    for col_name, dtype in df.dtypes.items():
        if dtype == 'object':
            first_val = df[col_name].dropna().iloc[0] if not df[col_name].isnull().all() else None
            if isinstance(first_val, dict):
                print(f"Detected JSON data in column '{col_name}'. Mapping to JSONB.")
                dtype_dict[col_name] = JSONB
    
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',  # CRITICAL CHANGE: We append data after truncating in the DAG.
        index=False,
        schema='raw',     # We can define a schema for our raw data
        dtype=dtype_dict
    )
    print(f"Appended data to table 'raw.{table_name}'.")


def run_etl():
    """
    The main ETL function to be called by the Airflow DAG.
    It finds all match data, processes it, and loads it into a 'raw' schema in Postgres.
    """
    print("Starting the Football Data ETL process...")

    # 1. Get database connection from Airflow
    # This is the secure, standard way to handle connections in Airflow.
    conn = BaseHook.get_connection('football_db')
    db_uri = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(db_uri)
    print("Successfully connected to PostgreSQL via Airflow connection.")

    # 2. Use the fixed data path from our Docker container
    # This path is defined by the 'volumes' in docker-compose.yml
    data_path = '/usr/local/airflow/data/Done'
    
    # 3. Walk through the entire data directory to process all matches
    # This is a more robust approach than your original loop.
    for tournament_dir in os.listdir(data_path):
        # ... and so on for season, gameweek...
        # For simplicity, we'll go straight to the match_id folders
        # In a real scenario, you'd loop through each level of the directory.
        
        # This is a simplified loop assuming we are in a season directory
        # A full implementation would use os.walk as discussed previously.
        
        # Let's assume a simplified structure for demonstration: data/Done/{match_id}/files...
        for match_id in os.listdir(data_path):
            match_path = os.path.join(data_path, match_id)
            if not os.path.isdir(match_path):
                continue
            
            print(f"--- Processing Match ID: {match_id} ---")
            
            # This is where your complex logic from insert_record.py goes.
            # You would find all 28 files, call your rdjson/rdcsv functions,
            # and build your 'all_data' dictionary of DataFrames.
            
            # For demonstration, we'll create a simple mock 'all_data' dictionary
            all_data = {
                "game_summary": mock_processing(f"{match_path}/summary.csv", match_id, match_id),
                "shots": mock_processing(f"{match_path}/shots.json", match_id, match_id)
                # ... you would have all 28 dataframes here
            }

            # 4. Loop through the processed dataframes and load them to Postgres
            for table_name, df in all_data.items():
                if df is not None and not df.empty:
                    try:
                        store_dataframe_to_postgres(df, table_name, engine)
                    except Exception as e:
                        print(f"Error storing table '{table_name}' for match {match_id}: {e}")

    print("Football Data ETL process finished successfully.")


# This allows the script to be run directly for testing if needed
if __name__ == "__main__":
    run_etl()
