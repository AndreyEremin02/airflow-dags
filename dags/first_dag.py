from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    message = Variable.get("test")
    print("Hello Airflow")
    print(message)

with DAG(
    "example_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
) as dag:

    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )
