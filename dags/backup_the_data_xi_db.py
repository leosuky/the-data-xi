from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
import logging, os
import subprocess
import include.scripts.etl_oci as etl_oci
from airflow.providers.postgres.hooks.postgres import PostgresHook
import oci

CONFIG = oci.config.from_file(file_location="/usr/local/airflow/.oci/config")
NOTIFICATION = oci.ons.NotificationDataPlaneClient(CONFIG)
TOPIC_ID = 'ocid1.onstopic.oc1.iad.amaaaaaad6m4taqax45toalajzw4abfbzfued6oid23h7rdjws53bbie7m5a'
OBJECT_STORAGE = oci.object_storage.ObjectStorageClient(CONFIG)
NAMESPACE = OBJECT_STORAGE.get_namespace().data
BUCKET_NAME = "the_data_xi_data_dump"

log = logging.getLogger(__name__)


@dag(
    description="Daily backup of The Data XI PostgreSQL database",
    schedule="0 2 * * *",  # Daily at 2 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # default_args=default_args,
    tags=["backup", "the_data_xi", "postgres"],
    default_args={
        'on_failure_callback': etl_oci.notify_on_failure,
        'on_success_callback': etl_oci.notify_on_success
    }
)
def backup_the_data_xi_db():

    # Get connection details securely
    def get_postgres_env():
        hook = PostgresHook(postgres_conn_id="the_data_xi_postgres")
        conn = hook.get_connection("the_data_xi_postgres")
        return {
            "PGHOST": conn.host,
            "PGPORT": str(conn.port),
            "PGUSER": conn.login,
            "PGPASSWORD": conn.password,
            "PGDATABASE": conn.schema,
        }

    # Generate timestamped backup filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"/usr/local/airflow/pg_backup/the_data_xi_{timestamp}.dump"
    PG_DUMP_CMD = f"""pg_dump -Fc \
            --host=$PGHOST \
            --port=$PGPORT \
            --username=$PGUSER \
            --dbname=$PGDATABASE \
            --file={backup_file}"""
    
    PG_DUMP_CONFIRM = f'ls -lh {backup_file}'
    PG_CLEANUP = f"""cd /usr/local/airflow/pg_backup && \
        ls -t the_data_xi_*.dump | tail -n +8 | xargs rm -f"""

    @task
    def pg_dump_backup():

        log.info(f"Starting backup to {backup_file}...")

        try:
            # Executes the PD_DUMP command, capturing output and checking for errors
            result = subprocess.run(
                PG_DUMP_CMD, shell=True, check=True, capture_output=True, 
                text=True, cwd='/usr/local/airflow',
                env=get_postgres_env()
            )

            # After pg_dump succeeds...
            log.info(f"Backup file created!! Now reading backup file: {backup_file}")
            with open(backup_file, "rb") as f:
                dump_data = f.read()
            
            log.info(f"Uploading backup to OCI: {backup_file}")
            log.info(f"Uploading {len(dump_data)} bytes to OCI...")

            file_name = backup_file.split('/')[-1]

            OBJECT_STORAGE.put_object(
                namespace_name=NAMESPACE,
                bucket_name=BUCKET_NAME,
                object_name=file_name,
                put_object_body=dump_data,
            )

            log.info("Backup uploaded successfully")

            log.info(f"✅ Backup completed: {backup_file}")
            # Optionally log pg_dump output for debugging
            # log.info(f"PD_DUMP STDOUT: \n{result.stdout}")
        except subprocess.CalledProcessError as e:
            log.error(f"Backup failed for file {backup_file}. Stderr: {e.stderr}")
            log.error(f"STDOUT:This is the Output\n {e.stdout}")
            raise Exception(f"Bash Execution Failure (pg_dump): {e}")
        
        
        # If the file was created, confirm it exists
        try:
            confirm = subprocess.run(
                PG_DUMP_CONFIRM, shell=True, check=True, capture_output=True, 
                text=True, cwd='/usr/local/airflow',
            )
            log.info(f"Confirmation Successful: \n{confirm.stdout}")

            log.info(f"Now cleaning backup...")
            os.remove(backup_file)
            log.info("Local backup file deleted.")

        except subprocess.CalledProcessError as e:
            log.error(f"An Error Occured\n {e.stdout}")
            raise Exception(f"Bash Execution Failure (pg_dump): {e}")
        

    pg_dump_backup()

backup_the_data_xi_db()