from airflow.decorators import dag, task
from datetime import datetime
from sqlalchemy import create_engine
from airflow.sdk import Connection
import logging
import subprocess

import include.scripts.etl_oci as etl_oci
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


log = logging.getLogger(__name__)

# DBT_COMMAND = "cd /usr/local/airflow/the_data_xi_dbt && dbt run --select staging --profiles-dir /usr/local/airflow/the_data_xi_dbt --profile the_data_xi_dbt"
DBT_COMMAND = "dbt run --select staging --profiles-dir /usr/local/airflow/the_data_xi_dbt"

@dag(
    description='Dag to extract data from files and load into postgresql',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['etl', 'the_data_xi', 'football', 'Oracle Cloud', 'OCI'],
    default_args={
        'postgres_conn_id': 'the_data_xi_postgres'
    }
)
def etl_copy_data_xi():

    # 1. INITIAL EXTRACTION TASK (Map Source)
    # This task runs once and returns a list/dict of items to iterate over.
    @task(task_id="get_all_match_prefixes")
    def get_all_match_prefixes_task() -> list[dict]:
        """Retrieves file names from OCI Bucket and validates file counts."""
        log.info("Retrieving file names from OCI Bucket")
        nested_files = etl_oci.get_file_names_from_bucket()
        
        # Transform the result into a list of dicts suitable for mapping
        map_list = []
        for prefix, files in nested_files.items():
            if len(files) < 28: # Reduced to 1 for generic safety; check your file count
                raise(f"Error!! The total files are less than 28 for Folder: {prefix}")
            
            # Extract combo_id (last element after splitting by /)
            combo_id = prefix.split('/')[-1]
            map_list.append({
                "combo_id": combo_id,
                "prefix": prefix,
                "files": files
            })
            
        return map_list
    
    # Run the retrieval task
    match_list = get_all_match_prefixes_task()

    @task(task_id="process_load_and_dbt_single_task")
    def process_load_and_dbt_single_task(match_info: dict):
        """
        Performs all E-T-L steps for a single match: 
        1. Downloads objects to memory.
        2. Processes and transforms dataframes (Python T).
        3. Loads dataframes to the PostgreSQL raw schema (L - Python).
        4. Runs dbt staging models on the new raw data (ELT - Bash).
        """
        combo_id = match_info["combo_id"]
        prefix = match_info["prefix"]
        files = match_info["files"]
        
        log.info(f"--- STARTING PROCESSING FOR MATCH: {combo_id} ---")
        
        # E/T: Download objects and process data
        # -------------------------------------------------
        log.info("Step 1: Downloading objects and processing dataframes...")
        all_files_in_memory = etl_oci.download_objects_to_memory(prefix, files)
        all_data_dataframes = etl_oci.process_data(combo_id, all_files_in_memory)
        log.info("Step 1 Complete: Dataframes prepared.")
        # -------------------------------------------------
        
        # L: Load Data to Raw Schema (Python)
        # -------------------------------------------------
        # hook = PostgresHook(postgres_conn_id="the_data_xi_postgres")
        
        # STEP 2 - PUSH DATA TO RAW SCHEMA
        # -------------------------------------------------
        log.info("Step 2: Pushing dataframes to Postgres raw schema...")
        
        for table_name, dataframe in all_data_dataframes.items():
            # First check if our schema matches
            try:
                conn = etl_oci.connect_to_postgres()
                etl_oci.ensure_schema_match_raw_table(dataframe, table_name, conn)
            except Exception as err:
                raise(f"An Error occured while checking schema match for table: {table_name}")
            
            # If Schema check succeeds, load data.
            try:
                conn = etl_oci.connect_to_postgres()
                etl_oci.store_dataframe_to_postgres(dataframe, table_name, conn=conn) 
            except Exception as err:
                log.error(f"Error storing table '{table_name}' for match {combo_id}: {err}")
                raise Exception(f"Python Load failure for match {combo_id}.")

        log.info(f"Step 2 Complete: Data pushed for match {combo_id}")
        # -------------------------------------------------

        # STEP 3 - DBT
        # -------------------------------------------------
        # ELT: Run dbt Staging (Bash via Subprocess)
        log.info("Step 3: Running dbt staging via bash subprocess...")
        
        try:
            # Executes the dbt command, capturing output and checking for errors
            result = subprocess.run(DBT_COMMAND, shell=True, check=True, 
                                    capture_output=True, text=True, cwd='/usr/local/airflow/the_data_xi_dbt')
            
            log.info("Step 3 Complete: DBT STAGING RUN SUCCESSFUL.")
            # Optionally log dbt output for debugging
            log.info(f"DBT STDOUT: \n{result.stdout}") 

        except subprocess.CalledProcessError as e:
            log.error(f"DBT run failed for match {combo_id}. Stderr: {e.stderr}")
            log.error(f"STDOUT:This is the Output\n {e.stdout}")
            raise Exception(f"Bash Execution Failure (dbt): {e}")

        log.info(f"--- MATCH {combo_id} FULLY PROCESSED AND STAGED ---")
        # return f"Match {combo_id} processed and staged."
        # -------------------------------------------------

        # STEP 4 - TRUNCATE RAW SCHEMA
        # -------------------------------------------------
        hook = PostgresHook(postgres_conn_id="the_data_xi_postgres")
        for table in all_data_dataframes.keys():
            try:
                hook.run(f"TRUNCATE TABLE raw.{table} RESTART IDENTITY CASCADE;")
                log.info(f'Table {table} successfully truncated')
            except Exception as err:
                log.error(f"An Error occured: {err}")

    # Map the unified task over the list of matches
    final_mapped_task = process_load_and_dbt_single_task.expand(match_info=match_list)

    final_mapped_task

etl_copy_data_xi()