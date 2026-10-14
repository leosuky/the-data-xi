"""
the_data_xi_selective_reload.py
===============================
MANUAL-ONLY reload DAG (schedule=None).

Re-runs a chosen SUBSET of providers/parsers over the matches in FIXTURES_DIR,
without touching anything else. Use it after a parser change, a new column, or
a new table — instead of reloading every provider and every table.

Why this is safe: every write in the loader is delete-then-insert scoped to
(table, combo_id). Running a subset rewrites ONLY that subset's tables; all
other raw tables for the match are left exactly as they were.

Typical uses
------------
  * changed parse_defending          -> providers=[whoscored], parsers=[defending]
  * added a column to ws_shots       -> providers=[whoscored], parsers=[shooting]
    (ALTER the table first, then reload — delete+insert rewrites the full row)
  * re-pull odds only                -> providers=[oddspedia]
  * rebuild game state               -> providers=[whoscored], parsers=[game_state]

Season selection is the directory: edit FIXTURES_DIR (or set the fixtures_dir
param) and run one season at a time, so failures stay easy to debug.

This DAG is deliberately separate from `the_data_xi_load` so the nightly load
keeps its own defaults and a partial reload can never fire on a schedule.
"""

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from datetime import datetime
import logging

from include.helpers.common.discovery import discover_unloaded_matches
from include.helpers.loader import load_match, PROVIDERS, WS_UNITS

log = logging.getLogger(__name__)

FIXTURES_DIR  = '/usr/local/airflow/data/EPL/2018_2019'
POSTGRES_CONN = 'the_data_xi_postgres'
TARGET_SCHEMA = 'raw'


@dag(
    dag_id='the_data_xi_selective_reload',
    description='Manual selective reload of chosen providers/parsers over one season',
    start_date=datetime(2025, 1, 1),
    schedule=None,                 # manual trigger ONLY — never runs on a timer
    catchup=False,
    max_active_tasks=4,
    tags=['the_data_xi', 'etl', 'football', 'reload', 'manual'],
    params={
        'providers': Param(
            list(PROVIDERS),
            type='array',
            items={'type': 'string', 'enum': list(PROVIDERS)},
            title='Providers',
            description='Which providers to reload. Leave all selected for a full reload.',
        ),
        'parsers': Param(
            list(WS_UNITS),
            type='array',
            items={'type': 'string', 'enum': list(WS_UNITS)},
            title='WhoScored units',
            description=(
                'Which WhoScored units to re-run. Only applies when whoscored is '
                'selected above. Side-tables ride with their parent: shooting '
                'also rewrites ws_shots; passing also rewrites ws_pass_map / '
                'ws_pass_network / ws_avg_positions; possession_advanced also '
                'rewrites ws_sequences.'
            ),
        ),
        'fixtures_dir': Param(
            '',
            type='string',
            title='Fixtures dir override',
            description=(
                f'Leave blank to use the DAG default ({FIXTURES_DIR}). Set it to '
                'reload a different season without editing the file.'
            ),
        ),
        'trigger_dbt': Param(
            True,
            type='boolean',
            title='Trigger dbt afterwards',
            description=(
                'Run the dbt DAG when the reload finishes. Turn OFF when reloading '
                'several seasons back-to-back, then rebuild marts once at the end.'
            ),
        ),
    },
    default_args={
        'owner': 'the_data_xi',
        'postgres_conn_id': POSTGRES_CONN,
    },
)
def the_data_xi_selective_reload():

    @task(task_id='discover_matches')
    def discover_matches(**context) -> list[dict]:
        """Every valid match in the fixtures dir (a reload always reprocesses)."""
        p = context['params']
        fixtures_dir = (p.get('fixtures_dir') or '').strip() or FIXTURES_DIR
        providers = p.get('providers') or list(PROVIDERS)
        parsers   = p.get('parsers') or list(WS_UNITS)

        log.info(f'Selective reload: dir={fixtures_dir}')
        log.info(f'  providers = {providers}')
        if 'whoscored' in providers:
            log.info(f'  ws units  = {parsers}')
        else:
            log.info('  ws units  = (whoscored not selected — no ws_* table touched)')

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
        conn = hook.get_conn()
        try:
            matches = discover_unloaded_matches(
                fixtures_dir=fixtures_dir,
                conn=conn,
                schema=TARGET_SCHEMA,
                force_reload=True,      # a reload always reprocesses what's on disk
            )
        finally:
            conn.close()

        log.info(f'{len(matches)} matches to reload from {fixtures_dir}')
        if not matches:
            log.warning(
                'No matches found — reload_single_match will be SKIPPED. Check '
                'that fixtures_dir points at the SEASON folder (its immediate '
                'subdirectories must be combo_ids).'
            )
        return matches

    @task(task_id='reload_single_match')
    def reload_single_match(match_descriptor: dict, **context):
        """Reload only the selected providers/parsers for one match."""
        p = context['params']
        providers = p.get('providers') or list(PROVIDERS)
        parsers   = p.get('parsers') or list(WS_UNITS)
        combo_id  = match_descriptor['combo_id']

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
        conn = hook.get_conn()
        try:
            load_match(
                match_descriptor=match_descriptor,
                conn=conn,
                schema=TARGET_SCHEMA,
                providers=providers,
                parsers=parsers,
            )
            conn.commit()
            log.info(f'[{combo_id}] reloaded ✓')
        except Exception as e:
            conn.rollback()
            log.error(f'[{combo_id}] reload failed: {e}')
            raise
        finally:
            conn.close()

    @task.short_circuit(task_id='check_trigger_dbt')
    def check_trigger_dbt(**context) -> bool:
        """Only continue to the dbt trigger when the param says so."""
        want = bool(context['params'].get('trigger_dbt', True))
        if not want:
            log.info('trigger_dbt=False — skipping dbt. Remember to run it after '
                     'your last season.')
        return want

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='the_data_xi_dbt',
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule='all_success',
    )

    match_list = discover_matches()
    reloaded = reload_single_match.expand(match_descriptor=match_list)
    reloaded >> check_trigger_dbt() >> trigger_dbt


the_data_xi_selective_reload()
