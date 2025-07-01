from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id="hello_world", start_date=datetime(2023, 1, 1), schedule_interval="@hourly", catchup=False) as dag:
    task = BashOperator(task_id="say_hello", bash_command="echo 'Hello, world!'")

