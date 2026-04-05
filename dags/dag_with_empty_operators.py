from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="empty_operators_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
) as dag:

    # старт
    start = EmptyOperator(task_id="start")

    # первый слой
    task_a = EmptyOperator(task_id="task_a")
    task_b = EmptyOperator(task_id="task_b")
    task_c = EmptyOperator(task_id="task_c")

    # второй слой (ветвление)
    task_a1 = EmptyOperator(task_id="task_a1")
    task_a2 = EmptyOperator(task_id="task_a2")

    task_b1 = EmptyOperator(task_id="task_b1")
    task_b2 = EmptyOperator(task_id="task_b2")

    task_c1 = EmptyOperator(task_id="task_c1")

    # третий слой
    task_a1_x = EmptyOperator(task_id="task_a1_x")
    task_a2_x = EmptyOperator(task_id="task_a2_x")

    task_b1_x = EmptyOperator(task_id="task_b1_x")
    task_b2_x = EmptyOperator(task_id="task_b2_x")

    task_c1_x = EmptyOperator(task_id="task_c1_x")

    # объединение
    join_1 = EmptyOperator(task_id="join_1")
    join_2 = EmptyOperator(task_id="join_2")

    # финал
    end = EmptyOperator(task_id="end")

    # зависимости
    start >> [task_a, task_b, task_c]

    task_a >> [task_a1, task_a2]
    task_b >> [task_b1, task_b2]
    task_c >> task_c1

    task_a1 >> task_a1_x
    task_a2 >> task_a2_x

    task_b1 >> task_b1_x
    task_b2 >> task_b2_x

    task_c1 >> task_c1_x

    [task_a1_x, task_a2_x] >> join_1
    [task_b1_x, task_b2_x, task_c1_x] >> join_2

    [join_1, join_2] >> end
