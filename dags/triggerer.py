from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'trigger-dagrun-dag',
    start_date=datetime(2023, 1, 9),
    max_active_runs=1,
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="dependent",
        conf={"date": "{{ dag_run.conf['date'] }}"},
        wait_for_completion=True
    )

    trigger_dependent_dag