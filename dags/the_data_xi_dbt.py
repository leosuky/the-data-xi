"""
the_data_xi_dbt.py
==================
dbt transformation DAG for The Data XI v2.

Triggered by the_data_xi_load after all matches are loaded.
Runs dbt in three stages: staging → intermediate → marts.
"""

from airflow.decorators import dag, task
from datetime import datetime
import subprocess
import logging

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = '/usr/local/airflow/the_data_xi_dbt'
DBT_PROFILES    = f'--profiles-dir {DBT_PROJECT_DIR}'


def _run_dbt(command: str, label: str):
    """Execute a dbt command via subprocess with logging."""
    full_cmd = f'{command} {DBT_PROFILES}'
    log.info(f'Running: {full_cmd}')
    try:
        result = subprocess.run(
            full_cmd, shell=True, check=True,
            capture_output=True, text=True, cwd=DBT_PROJECT_DIR,
        )
        log.info(f'{label} completed successfully')
        log.debug(f'dbt stdout:\n{result.stdout}')
    except subprocess.CalledProcessError as e:
        log.error(f'{label} failed.\nstderr: {e.stderr}\nstdout: {e.stdout}')
        raise


@dag(
    dag_id='the_data_xi_dbt',
    description='Run dbt staging → intermediate → marts transformations',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['the_data_xi', 'dbt', 'transformation'],
    default_args={'owner': 'the_data_xi'},
)
def the_data_xi_dbt():

    @task(task_id='dbt_staging')
    def run_staging():
        _run_dbt('dbt run --select staging', 'dbt staging run')
        _run_dbt('dbt test --select staging', 'dbt staging test')

    @task(task_id='dbt_intermediate')
    def run_intermediate():
        _run_dbt('dbt run --select intermediate', 'dbt intermediate run')
        _run_dbt('dbt test --select intermediate', 'dbt intermediate test')

    @task(task_id='dbt_marts')
    def run_marts():
        _run_dbt('dbt run --select marts', 'dbt marts run')
        _run_dbt('dbt test --select marts', 'dbt marts test')

    run_staging() >> run_intermediate() >> run_marts()


the_data_xi_dbt()