from airflow.decorators import dag, task
# from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
from airflow.sdk import Connection
import os

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
    
    @task
    def task_2(value):
        xd = Connection.get("the_data_xi_postgres")

        print(f"{xd.host}@{xd.port}/{xd.login}:{xd.password}/{xd.schema}")

        # engine = create_engine(f"postgresql://{value['USER']}:{value['PASSWORD']}@{value['HOST']}:{value['PORT']}/{value['DB_NAME']}")
        uri = xd.get_uri()
        if uri.startswith("postgres://"):
            uri = uri.replace("postgres://", "postgresql://", 1)
            
        engine = create_engine(uri)

        try:
            conn = engine.connect()
            print(conn)
            print("Connection to Database was successful")

            conn.close()
        except Exception as e:
            print(e)

    task_2(task_1())

dummy_dag()