from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

FILE_PATH = "/opt/airflow/data/trigger.txt"

with DAG(
    dag_id="create_trigger_file",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
) as dag:

    create_dir_and_file = BashOperator(
        task_id="create_dir_and_file",
        bash_command=f"""
        mkdir -p /opt/airflow/data
        echo "trigger file created at $(date)" > {FILE_PATH}
        """
    )
