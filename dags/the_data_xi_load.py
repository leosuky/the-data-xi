"""
the_data_xi_load.py
===================
Main Airflow DAG for The Data XI v2 load pipeline.

Discovers unprocessed matches across the fixtures directory tree, then
dynamically maps a load task per match. Each task runs all provider
parsers and pushes to the RAW schema.

After all matches are loaded, triggers the dbt transformation DAG.

Trigger with the `force_reload` param set to True to reprocess ALL discovered
matches even if they are already in the DB (e.g. after a parser/schema change).
The loader is idempotent, so re-loading overwrites cleanly.
"""

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from datetime import datetime
import logging

from include.helpers.common.discovery import discover_unloaded_matches
from include.helpers.loader import load_match

log = logging.getLogger(__name__)

FIXTURES_DIR    = '/usr/local/airflow/data/fixtures'
POSTGRES_CONN   = 'the_data_xi_postgres'
TARGET_SCHEMA   = 'raw'


@dag(
    dag_id='the_data_xi_load',
    description='Discover and load match data from WhoScored, Fotmob, Sofascore, and Oddspedia',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    max_active_tasks=4,
    tags=['the_data_xi', 'etl', 'football', 'v2'],
    params={
        'force_reload': Param(
            True,
            type='boolean',
            title='Force reload',
            description=(
                'Reprocess ALL discovered matches even if they are already in '
                'the DB. Use after a parser or schema change. The loader is '
                'idempotent (upsert + delete-by-combo_id), so this is safe.'
            ),
        ),
    },
    default_args={
        'owner': 'the_data_xi',
        'postgres_conn_id': POSTGRES_CONN,
    },
)
def the_data_xi_load():

    @task(task_id='discover_matches')
    def discover_matches(**context) -> list[dict]:
        """Scan fixtures dir, diff against DB, return matches to load.

        When the `force_reload` param is True, the DB diff is bypassed and every
        discovered match is returned for reprocessing.
        """
        force_reload = bool(context['params'].get('force_reload', False))
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
        conn = hook.get_conn()
        try:
            matches = discover_unloaded_matches(
                fixtures_dir=FIXTURES_DIR,
                conn=conn,
                schema=TARGET_SCHEMA,
                force_reload=force_reload,
            )
        finally:
            conn.close()
        log.info(f'Discovered {len(matches)} matches to load (force_reload={force_reload})')
        if not matches:
            log.warning(
                'No matches to load — load_single_match will be SKIPPED (empty '
                'mapping). If you changed a parser, re-trigger with force_reload=True.'
            )
        return matches

    @task(task_id='load_single_match')
    def load_single_match(match_descriptor: dict):
        """Run the full load pipeline for a single match across all providers."""
        combo_id = match_descriptor['combo_id']
        log.info(f'[{combo_id}] Starting load pipeline')

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
        conn = hook.get_conn()
        try:
            load_match(
                match_descriptor=match_descriptor,
                conn=conn,
                schema=TARGET_SCHEMA,
            )
            conn.commit()
            log.info(f'[{combo_id}] Complete ✓')
        except Exception as e:
            conn.rollback()
            log.error(f'[{combo_id}] Failed: {e}')
            raise
        finally:
            conn.close()

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='the_data_xi_dbt',
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule='all_success',
    )

    match_list = discover_matches()
    loaded = load_single_match.expand(match_descriptor=match_list)
    loaded >> trigger_dbt


the_data_xi_load()