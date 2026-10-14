"""
the_data_xi_player_meta.py
==========================
MANUAL-ONLY player metadata refresh (schedule=None).

Loads scraped Fotmob PLAYER PAGES (fotmob_{player_id}.json) into the raw
player-meta tables. This is the source of the granular positional data the
match feeds don't carry: per-position appearance counts, the main-position
flag, and pitch coordinates.

Run it once at the end of each season (or after a transfer window) — the data
is player-grain and slow-moving, so there's nothing to schedule.

Grain
-----
Unlike the match DAGs, this one maps over PLAYERS, not matches. The natural key
is fotmob_player_id, which joins to int_player_xwalk.fot_player_id and on to
player_uid.

Coaches
-------
A retired player who is now a coach has no positional breakdown. The parser
detects that (four redundant signals) and emits a meta row with is_coach=true
and zero position rows — so ex-players-turned-coaches load cleanly instead of
failing or, worse, landing a fake 'Coach' position.

Expected layout:
    {PLAYERS_DIR}/
        fotmob_30613.json
        fotmob_883080.json
        ...
"""

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from datetime import datetime
import logging

from include.helpers.player_loader import discover_player_files, load_player

log = logging.getLogger(__name__)

PLAYERS_DIR   = '/usr/local/airflow/data/players'
POSTGRES_CONN = 'the_data_xi_postgres'
TARGET_SCHEMA = 'raw'


@dag(
    dag_id='the_data_xi_player_meta',
    description='Manual refresh of player metadata + granular positions from Fotmob player pages',
    start_date=datetime(2025, 1, 1),
    schedule=None,                 # manual only — run at season end
    catchup=False,
    max_active_tasks=4,
    tags=['the_data_xi', 'etl', 'football', 'players', 'manual'],
    params={
        'players_dir': Param(
            '',
            type='string',
            title='Players dir override',
            description=f'Leave blank to use the DAG default ({PLAYERS_DIR}).',
        ),
        'trigger_dbt': Param(
            True,
            type='boolean',
            title='Trigger dbt afterwards',
            description='Rebuild the marts once the player load finishes.',
        ),
    },
    default_args={
        'owner': 'the_data_xi',
        'postgres_conn_id': POSTGRES_CONN,
    },
)
def the_data_xi_player_meta():

    @task(task_id='discover_players')
    def discover_players(**context) -> list[dict]:
        """Scan the players directory for fotmob_{id}.json files."""
        players_dir = (context['params'].get('players_dir') or '').strip() or PLAYERS_DIR
        players = discover_player_files(players_dir)
        log.info(f'{len(players)} player files to load from {players_dir}')
        if not players:
            log.warning(
                'No player files found — load_single_player will be SKIPPED. '
                'Files must be named fotmob_{player_id}.json.'
            )
        return players

    @task(task_id='load_single_player')
    def load_single_player(descriptor: dict):
        """Parse and load one player page (coach-safe)."""
        pid = descriptor['fotmob_player_id']
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
        conn = hook.get_conn()
        try:
            load_player(descriptor=descriptor, conn=conn, schema=TARGET_SCHEMA)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error(f'[{pid}] player load failed: {e}')
            raise
        finally:
            conn.close()

    @task.short_circuit(task_id='check_trigger_dbt')
    def check_trigger_dbt(**context) -> bool:
        want = bool(context['params'].get('trigger_dbt', True))
        if not want:
            log.info('trigger_dbt=False — skipping dbt.')
        return want

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='the_data_xi_dbt',
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule='all_success',
    )

    player_list = discover_players()
    loaded = load_single_player.expand(descriptor=player_list)
    loaded >> check_trigger_dbt() >> trigger_dbt


the_data_xi_player_meta()
