from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task
from datetime import datetime
import logging
import subprocess
import include.scripts.etl_local as etl_local
from airflow.providers.postgres.hooks.postgres import PostgresHook


log = logging.getLogger(__name__)

DBT_COMPILE = "dbt compile --select staging --profiles-dir /usr/local/airflow/the_data_xi_dbt"
DBT_RUN = "dbt run --select staging --profiles-dir /usr/local/airflow/the_data_xi_dbt"
BASE_DIR = '/usr/local/airflow/data'

@dag(
    description='Dag to extract data from files and load into postgresql',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['etl', 'the_data_xi', 'football', 'local storage'],
    default_args={
        'postgres_conn_id': 'the_data_xi_postgres',
        # 'on_failure_callback': etl_oci.notify_oci,
        # 'on_success_callback': etl_oci.notify_oci
    }
)
def the_data_xi_extract_load_local():

    # 1. INITIAL EXTRACTION TASK (Map Source)
    # This task runs once and returns a list/dict of items to iterate over.
    @task(task_id="get_all_match_paths")
    def get_all_match_paths_task() -> list[dict]:
        """Retrieves file names from local directory and validates file counts."""
        log.info("Retrieving file names from local directory")
        map_list = etl_local.get_file_names_from_local_dir(BASE_DIR)
            
        return map_list
    
    # Run the retrieval task
    match_list = get_all_match_paths_task()

    @task(task_id="process_load_and_dbt_single_task")
    def process_load_and_dbt_single_task(match: dict):
        """
        Performs all E-T-L steps for a single match: 
        1. Processes and transforms dataframes (Python T).
        2. Loads dataframes to the PostgreSQL raw schema (L - Python).
        3. Runs dbt staging models on the new raw data (ELT - Bash).
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

        # STEP 3 - DBT
        # -------------------------------------------------
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
            log.error(f"DBT run failed for match {combo_id}. Stderr: {e.stderr}")
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
    final_mapped_task = process_load_and_dbt_single_task.expand(match=match_list)

    # @task
    # def move_data_to_processed_bucket(match_info: list):
        
    #     for game in match_info:
    #         combo_id = game["combo_id"]
    #         prefix = game["prefix"]
    #         files = game["files"]

    #         for file in files:
    #             object_name = f'{prefix}/{file}'
    #             etl_oci.move_file_between_folders(object_name)

    #         log.info(f'Successfully moved all files for {combo_id}')


     # NEW: Trigger the marts DAG after all matches are processed
    trigger_marts_dag = TriggerDagRunOperator(
        task_id="trigger_marts_dag",
        trigger_dag_id="dbt_marts_dag",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule="all_success"
    )


    # After all files are processed, move them.
    # final_mapped_task >> move_data_to_processed_bucket(match_list) >> trigger_marts_dag
    final_mapped_task >> trigger_marts_dag

the_data_xi_extract_load_local()