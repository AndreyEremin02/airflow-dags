from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def work(task_name):
    print(f"Start {task_name}")
    time.sleep(10)
    print(f"Finish {task_name}")


with DAG(
    dag_id="pool_demo_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "pool"],
) as dag:

    start = EmptyOperator(task_id="start")

    tasks = []

    for i in range(1, 6):
        task = PythonOperator(
            task_id=f"task_{i}",
            python_callable=work,
            op_kwargs={"task_name": f"task_{i}"},
            pool="demo_pool",
        )
        tasks.append(task)

    end = EmptyOperator(task_id="end")

    start >> tasks >> end
