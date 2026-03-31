from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from datetime import datetime

numbers_dataset = Dataset("postgres://airflow-test-db.airflow-test.svc.cluster.local:5432/test_db/public/numbers")

def calculate_avg():
    hook = PostgresHook(postgres_conn_id='postgres_default')

    records = hook.get_records("SELECT value FROM numbers")
    values = [r[0] for r in records]

    avg = sum(values) / len(values)

    hook.run("""
        CREATE TABLE IF NOT EXISTS result (
            avg_value FLOAT
        );

        TRUNCATE TABLE result;
    """)

    hook.run(
        "INSERT INTO result (avg_value) VALUES (%s)",
        parameters=(avg,)
    )

with DAG(
    dag_id="consumer_dag",
    start_date=datetime(2024, 1, 1),
    schedule=[numbers_dataset],
    catchup=False
) as dag:

    calc_task = PythonOperator(
        task_id="calculate_avg",
        python_callable=calculate_avg
    )
