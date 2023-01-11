from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def test_func(**context):
    print(context["dag_run"].conf["date"])


with DAG(
    'dependent',
    start_date=datetime(2023, 1, 9),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='task',
        python_callable=test_func,
        provide_context=True,
    )

    task
