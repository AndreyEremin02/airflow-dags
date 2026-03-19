from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello Airflow from second DAG")

with DAG(
    "example_dag2",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
) as dag:

    task = PythonOperator(
        task_id="hello_task2",
        python_callable=hello
    )
