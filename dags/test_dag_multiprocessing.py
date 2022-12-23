import time
import random
import uuid

from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


TASKS = [f"{str(uuid.uuid4())}.jpg" for _ in range(100)]


def process_task():
    print("Starting process...")
    time.sleep(random.randint(0, 20))
    print("Done!")


with DAG(
        "test_dag_multiprocessing",
        description="test multiprocessing",
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': days_ago(1),
            'schedule_interval': None,
        },
        template_searchpath=["/"],
        catchup=False,
) as dag:
    @task
    def get_config(**context):
        workers = context['dag_run'].conf.get('workers', default=None)
        print(workers)
        print(int(workers))
        print(workers.isdigit())
        print("Default to 1 worker.")
        return 1

    NUM_WORKERS = get_config()

    start = BashOperator(task_id="start", bash_command="echo Starting DAG")
    collect = BashOperator(task_id="collect", bash_command="echo Collecting multiprocessing results")
    end = BashOperator(task_id="end", bash_command="echo Ending DAG")

    blur_tasks = [
        PythonOperator(task_id=f"multiprocessing_blur_{i}", python_callable=process_task) for i in range(NUM_WORKERS)]

    with TaskGroup("multiprocessing_detection") as multiprocessing_detection:
        for i in range(NUM_WORKERS):
            PythonOperator(task_id=f"multiprocessing_detection_{i}", python_callable=process_task)

    start >> blur_tasks >> collect >> multiprocessing_detection >> end
