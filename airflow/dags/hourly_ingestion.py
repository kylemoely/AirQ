from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import os

default_args = {
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
        }

env_vars = {
        "DB_USER": os.environ["DB_USER"],
        "DB_NAME": os.environ["DB_NAME"],
        "DB_PORT": os.environ["DB_PORT"],
        "DB_HOST": os.environ["DB_HOST"],
        "DB_PASSWORD": os.environ["DB_PASSWORD"],
        "OPENAQ_API_BASE": os.environ["OPENAQ_API_BASE"],
        "DATA_DIR": os.environ["DATA_DIR"],
        "API_KEY": os.environ["API_KEY"]
        }

with DAG(dag_id="hourly_ingestion", start_date=datetime(2023, 1, 1), schedule_interval="@hourly", catchup=False, default_args=default_args) as dag:
    task = DockerOperator(task_id="run_hourly_ingestion", image="hourly_ingestion", command="python run.py", environment=env_vars, network_mode="bridge", auto_remove="never")
