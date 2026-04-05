from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

def process_file():
    print("Processing file")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="file_sensor_full_demo",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "sensor"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/data/trigger.txt",
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    process = PythonOperator(
        task_id="process_file",
        python_callable=process_file,
    )

    wait_for_file >> process
