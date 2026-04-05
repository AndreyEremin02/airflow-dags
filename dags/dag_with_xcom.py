from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    data = {"numbers": [1, 2, 3, 4, 5]}
    print(f"Extracted: {data}")
    return data

def transform(**context):
    ti = context["ti"]

    data = ti.xcom_pull(task_ids="extract_task")

    numbers = data["numbers"]
    result = sum(numbers)

    print(f"Sum: {result}")
    return result

def load(**context):
    ti = context["ti"]

    result = ti.xcom_pull(task_ids="transform_task")

    print(f"Final result: {result}")

with DAG(
    dag_id="xcom_demo_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    # зависимости
    extract_task >> transform_task >> load_task
