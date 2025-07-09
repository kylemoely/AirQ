from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from dotenv import dotenv_values

default_args = {
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
        }

with DAG(dag_id="hourly_ingestion", start_date=datetime(2023, 1, 1), schedule_interval="@hourly", catchup=False, default_args=default_args) as dag:
    task = DockerOperator(task_id="run_hourly_ingestion", image="hourly_ingestion", command="python run.py", environment=dotenv_values("../.env"), network_mode="bridge", auto_remove="never")
