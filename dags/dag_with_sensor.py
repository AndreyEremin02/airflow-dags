from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

FILE_PATH = "/opt/airflow/data/trigger.txt"


def process_file():
    print("Processing file")


def delete_file():
    if os.path.exists(FILE_PATH):
        os.remove(FILE_PATH)
        print(f"File deleted: {FILE_PATH}")
    else:
        print("File not found, nothing to delete")


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
        filepath="trigger.txt",
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    process = PythonOperator(
        task_id="process_file",
        python_callable=process_file,
    )

    cleanup = PythonOperator(
        task_id="delete_file",
        python_callable=delete_file,
    )

    wait_for_file >> process >> cleanup
