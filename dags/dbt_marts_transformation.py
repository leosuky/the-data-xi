from airflow.decorators import dag, task
from datetime import datetime
import subprocess
import include.scripts.etl_oci as etl_oci

# Define the dbt command to run the marts models
DBT_RUN_INTER = "dbt run --select intermediate"
DBT_RUN_MARTS = "dbt run --select marts"
DBT_TEST_MARTS = "dbt test --select marts"
DBT_PROJECT_DIR = "/usr/local/airflow/the_data_xi_dbt" # Path to your dbt project

@dag(
    dag_id='dbt_marts_dag',
    description='A DAG to run dbt models for the marts schema.',
    start_date=datetime(2025, 1, 1),
    schedule=None,  # This DAG will be triggered by another DAG
    catchup=False,
    tags=['dbt', 'marts', 'the_data_xi'],
    default_args={
        'owner': 'airflow',
        # Best practice: point dbt profiles dir to a path inside the project
        'env': {'DBT_PROFILES_DIR': '.'},
        'on_failure_callback': etl_oci.notify_on_failure,
        'on_success_callback': etl_oci.notify_on_success
    }
)
def dbt_marts_dag():

    @task(task_id="run_dbt_intermediate")
    def run_dbt_marts_task():
        """
        Executes the dbt run command to build the intermediate views.
        The command is targeted to only run models in the 'intermediate' directory.
        """
        print(f"Executing command: {DBT_RUN_INTER}")
        try:
            result = subprocess.run(
                DBT_RUN_MARTS,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                cwd=DBT_PROJECT_DIR
            )
            print("dbt run command executed successfully.")
            print(f"dbt STDOUT: \n{result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"dbt run command failed. Stderr: {e.stderr}")
            print(f"dbt STDOUT: \n{e.stdout}")
            raise

    @task(task_id="run_dbt_marts")
    def run_dbt_marts_task():
        """
        Executes the dbt run command to build the mart tables.
        The command is targeted to only run models in the 'marts' directory.
        """
        print(f"Executing command: {DBT_RUN_MARTS}")
        try:
            result = subprocess.run(
                DBT_RUN_MARTS,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                cwd=DBT_PROJECT_DIR
            )
            print("dbt run command executed successfully.")
            print(f"dbt STDOUT: \n{result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"dbt run command failed. Stderr: {e.stderr}")
            print(f"dbt STDOUT: \n{e.stdout}")
            raise

    @task(task_id="test_dbt_marts")
    def test_dbt_marts_task():
        """
        Executes the dbt test command on the newly created mart tables
        to ensure data quality and integrity.
        """
        print(f"Executing command: {DBT_TEST_MARTS}")
        try:
            result = subprocess.run(
                DBT_TEST_MARTS,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                cwd=DBT_PROJECT_DIR
            )
            print("dbt test command executed successfully.")
            print(f"dbt STDOUT: \n{result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"dbt test command failed. Stderr: {e.stderr}")
            print(f"dbt STDOUT: \n{e.stdout}")
            raise

    # Define the task dependencies
    run_dbt_marts_task() >> test_dbt_marts_task()

dbt_marts_dag_instance = dbt_marts_dag()

