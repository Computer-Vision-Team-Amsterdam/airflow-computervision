from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, time

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def trigger_generator(**ctx):
    n_runs = 4
    first_run = datetime.combine(
        date=datetime.strptime(ctx["dag_run"].conf["date"], "%Y-%m-%d"),
        time=time(hour=21),
    )
    trigger_times = [first_run + timedelta(hours=x * 2) for x in range(n_runs)]
    arguments = [first_run + timedelta(minutes=x) for x in range(n_runs)]
    return zip(trigger_times, arguments)


with DAG(
    "trigger-dagrun-dag",
    start_date=datetime(2023, 1, 9),
    max_active_runs=1,
    schedule_interval="0 22 * * 1-5",
    default_args=default_args,
    catchup=False
) as dag:
    trigs = [
        TriggerDagRunOperator(
            task_id=f"trigger_dependent_dag_{x}",
            trigger_dag_id="dependent",
            wait_for_completion=True,
            execution_date=f"{{{{ data_interval_start.add(hours={x} * 2) }}}}",
            conf={"date": f"{{{{ ds ~ ' 21:{x}0:00' }}}}"},
        )
    for x in range(4)]
    trigs
