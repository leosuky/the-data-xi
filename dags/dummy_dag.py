from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import oci
import os

PROFILES_DIR = '/home/astro/.dbt'
DBT_PROJECT_ROOT = "/usr/local/airflow/the_data_xi_dbt"

@dag(
    description='A dag used to test the functionality of things',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['dummy functions']
)
def dummy_dag():
    
    @task
    def task_1():
        HOST=os.environ.get("POSTGRES_HOST")
        PORT=os.environ.get("POSTGRES_PORT")
        DB_NAME=os.environ.get("POSTGRES_DB")
        DB_USER=os.environ.get("POSTGRES_USER")
        DB_PASSWORD=os.environ.get("POSTGRES_PASSWORD")

        print(f"{HOST}@{PORT}/{DB_USER}:{DB_PASSWORD}/{DB_NAME}")
        return {
            "HOST": HOST,
            "PORT": PORT,
            "DB_NAME": DB_NAME,
            "PASSWORD": DB_PASSWORD,
            "USER": DB_USER
        }
    
    # run_dbt_model = BashOperator(
    #     task_id='run_dbt_model',
    #     # bash_command='cd /usr/local/airflow/the_data_xi_dbt && dbt debug --profiles-dir /usr/local/airflow/the_data_xi_dbt',
    #     bash_command='dbt debug',
    #     cwd='/usr/local/airflow/the_data_xi_dbt'
    # )
    @task
    def task2():
        hook = PostgresHook(postgres_conn_id="the_data_xi_postgres")
        try:
            xd = hook.get_records(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'raw' AND table_name   = 'misc_json_data';")
            print(f'Table  == {xd}')
        except Exception as err:
            print(f"An Error occured: {err}")

    task_1() >> task2()

dummy_dag()