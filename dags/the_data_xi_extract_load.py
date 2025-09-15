from airflow.decorators import dag, task
from datetime import datetime
from sqlalchemy import create_engine
from airflow.sdk import Connection
import os

from include.scripts.etl import run_etl
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


RAW_TABLES = [
        "game_summary",
        "advanced_pass_types",
        "advanced_passing",
        "advanced_defending",
        "advanced_possession",
        "misc_stats",
        "gk_stats",
        "shots",
        "advanced_shot_data",
        "match_stats",
        "lineup",
        "player_stats",
        "odds_data",
        "match_details",
        "managers",
        "referee",
        "teams",
        "player_data",
        "misc_json_data"
    ]

BASE_DIR = './data'

@dag(
    description='Dag to extract data from files and load into postgresql',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['etl', 'the_data_xi', 'football']
)
def the_data_xi_extract_load():

    @task
    def truncate_tables_before_insert():
        hook = PostgresHook(postgres_conn_id="the_data_xi_postgres")
        for table in RAW_TABLES:
            try:
                hook.run(f"TRUNCATE TABLE raw.{table} RESTART IDENTITY CASCADE;")
            except Exception as err:
                print(f"An Error occured: {err}")


    @task
    def extract_files_to_postgres():
        run_etl(BASE_DIR)


    truncate_tables_before_insert() >> extract_files_to_postgres()

the_data_xi_extract_load()