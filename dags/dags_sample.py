from airflow import DAG
from airflow.operators.empty import EmptyOperator  # Используем EmptyOperator вместо DummyOperator
from datetime import datetime

# Создаем DAG
with DAG(
    dag_id='example_dag_with_complex_dependencies',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Используем `schedule` вместо `schedule_interval`
    catchup=False
) as dag:

    # Создаем "левые" задачи
    task_left_1 = EmptyOperator(task_id="task_left_1")
    task_left_2 = EmptyOperator(task_id="task_left_2")
    task_left_3 = EmptyOperator(task_id="task_left_3")
    task_left_4 = EmptyOperator(task_id="task_left_4")

    # Создаем центральную задачу-концентратор
    central_task = EmptyOperator(task_id="central_task")

    # Создаем "правые" задачи
    task_right_1 = EmptyOperator(task_id="task_right_1")
    task_right_2 = EmptyOperator(task_id="task_right_2")
    task_right_3 = EmptyOperator(task_id="task_right_3")
    task_right_4 = EmptyOperator(task_id="task_right_4")

    # Задаем зависимости от левых задач к центральной задаче
    [task_left_1, task_left_2, task_left_3, task_left_4] >> central_task

    # Задаем зависимости от центральной задачи к правым задачам
    central_task >> [task_right_1, task_right_2, task_right_3, task_right_4]

