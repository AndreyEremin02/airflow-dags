from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.datasets import Dataset

numbers_dataset = Dataset("postgres://airflow-test-db.airflow-test.svc.cluster.local:5432/test_db/public/numbers")

def update_numbers():
    hook = PostgresHook(postgres_conn_id='postgres-production')

    hook.run("""
        TRUNCATE TABLE numbers;

        INSERT INTO numbers (value) VALUES
        (5), (15), (25), (35), (45);
    """)

with DAG(
    dag_id="producer_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    update_task = PythonOperator(
        task_id="update_numbers",
        python_callable=update_numbers,
        outlets=[numbers_dataset]
    )
