from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task
from datetime import datetime
import logging
import subprocess
import include.scripts.etl_local as etl_local
import include.scripts.etl_oci as etl_oci
from airflow.providers.postgres.hooks.postgres import PostgresHook


log = logging.getLogger(__name__)

DBT_COMPILE = "dbt compile --select staging --profiles-dir /usr/local/airflow/the_data_xi_dbt"
DBT_RUN = "dbt run --select staging --profiles-dir /usr/local/airflow/the_data_xi_dbt"
BASE_DIR = '/usr/local/airflow/data/xdr'
RAW_TABLES = [
    "game_summary", "advanced_pass_types", "advanced_passing", "advanced_defending",
    "advanced_possession", "misc_stats", "gk_stats", "shots", "advanced_shot_data",
    "match_stats", "lineup", "player_stats", "odds_data", "match_details",
    "managers", "referee", "teams", "player_data", "misc_json_data", "missing_players",
    "seasons", "tournaments"

]

@dag(
    description='Dag to extract data from files and load into postgresql',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['etl', 'the_data_xi', 'football', 'local storage'],
    on_failure_callback=etl_oci.notify_on_failure,
    on_success_callback=etl_oci.notify_on_success,
    default_args={
        'postgres_conn_id': 'the_data_xi_postgres',
    }
)
def the_data_xi_extract_load_local():


    # TASK 1: Pre-truncate Raw Tables
    @task(task_id="truncate_raw_pre_load")
    def truncate_raw_pre():
        hook = PostgresHook(postgres_conn_id="the_data_xi_postgres")
        for table in RAW_TABLES:
            try:
                hook.run(f"TRUNCATE TABLE raw.{table} RESTART IDENTITY CASCADE;")
                log.info(f'Table {table} successfully truncated')
            except Exception as err:
                log.error(f"An Error occured: {err}")


    # TASK 2. INITIAL EXTRACTION TASK (Map Source)
    # This task runs once and returns a list/dict of items to iterate over.
    @task(task_id="get_all_match_paths")
    def get_all_match_paths_task() -> list[dict]:
        """Retrieves file names from local directory and validates file counts."""
        log.info("Retrieving file names from local directory")
        map_list = etl_local.get_file_names_from_local_dir(BASE_DIR)
            
        return map_list
    
    # Truncate and Run the retrieval task
    pre_trunc = truncate_raw_pre()
    match_list = get_all_match_paths_task()

    # TASK 3 - LOAD ALL MATCHES TO RAW SCHEMA
    @task(task_id="load_match_to_raw")
    def load_match_to_raw(match: dict):
        """
        Performs all E-T-L steps for a single match: 
        1. Processes and transforms dataframes (Python T).
        2. Loads dataframes to the PostgreSQL raw schema (L - Python).
        """

        combo_id = match['combo_id']
        
        # E/T: Download objects and process data
        # -------------------------------------------------
        log.info("Step 1: Processing dataframes...")
        all_data_dataframes = etl_local.process_data(match)
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
                conn = etl_local.connect_to_postgres()
                etl_local.ensure_schema_match_raw_table(dataframe, table_name, conn)
            except Exception as err:
                raise(f"An Error occured while checking schema match for table: {table_name}")
            
            # If Schema check succeeds, load data.
            try:
                conn = etl_local.connect_to_postgres()
                etl_local.store_dataframe_to_postgres(dataframe, table_name, conn=conn) 
            except Exception as err:
                log.error(f"Error storing table '{table_name}' for match {combo_id}: {err}")
                raise Exception(f"Python Load failure for match {combo_id}.")

        log.info(f"Step 2 Complete: Data pushed for match {combo_id}")
        # -------------------------------------------------

    # TASK 4 - DBT RUN STAGING
    @task(task_id="run_dbt_staging")
    def run_dbt_staging():

        # ELT: Run dbt Staging (Bash via Subprocess)
        log.info("Step 3: Running dbt staging via bash subprocess...")

        # COMPILE 
        try:
            # Executes the dbt command, capturing output and checking for errors
            result = subprocess.run(DBT_COMPILE, shell=True, check=True, 
                                    capture_output=True, text=True, cwd='/usr/local/airflow/the_data_xi_dbt')
            
            log.info("Step 3 Complete: DBT STAGING COMPILED SUCCESSFUL.")
            # Optionally log dbt output for debugging
            log.info(f"DBT STDOUT: \n{result.stdout}") 

        except subprocess.CalledProcessError as e:
            log.error(f"DBT run failed. Stderr: {e.stderr}")
            log.error(f"STDOUT:This is the Output\n {e.stdout}")
            raise Exception(f"Bash Execution Failure (dbt): {e}")
        
        # LOAD THE DATA
        try:
            # Executes the dbt command, capturing output and checking for errors
            result = subprocess.run(DBT_RUN, shell=True, check=True, 
                                    capture_output=True, text=True, cwd='/usr/local/airflow/the_data_xi_dbt')
            
            log.info("Step 4 Complete: DBT STAGING RUN SUCCESSFUL.")
            # Optionally log dbt output for debugging
            log.info(f"DBT STDOUT: \n{result.stdout}") 

        except subprocess.CalledProcessError as e:
            log.error(f"DBT run failed. Stderr: {e.stderr}")
            log.error(f"STDOUT:This is the Output\n {e.stdout}")
            raise Exception(f"Bash Execution Failure (dbt): {e}")

        log.info(f"--- ALL MATCHES FULLY PROCESSED AND STAGED ---")
        # return f"Match {combo_id} processed and staged."
        # -------------------------------------------------

    # TASK 5: truncate Raw Tables after DBT RUN
    @task(task_id="truncate_raw_post_load")
    def truncate_raw_post_load():
        hook = PostgresHook(postgres_conn_id="the_data_xi_postgres")
        for table in RAW_TABLES:
            try:
                hook.run(f"TRUNCATE TABLE raw.{table} RESTART IDENTITY CASCADE;")
                log.info(f'Table {table} successfully truncated')
            except Exception as err:
                log.error(f"An Error occured: {err}")
    

     # NEW: Trigger the marts DAG after all matches are processed
    trigger_marts_dag = TriggerDagRunOperator(
        task_id="trigger_marts_dag",
        trigger_dag_id="dbt_marts_dag",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule="all_success"
    )


    pre_trunc >> match_list

    # Map the unified task over the list of matches
    load_raw_all = load_match_to_raw.expand(match=match_list)

    load_raw_all >> run_dbt_staging() >> truncate_raw_post_load() >> trigger_marts_dag

the_data_xi_extract_load_local()