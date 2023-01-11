from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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


with DAG(
    "trigger-dagrun-dag",
    start_date=datetime(2023, 1, 9),
    max_active_runs=1,
    schedule_interval="0 22 * * 3",  # Every Wednesday at 22:00 UTC
    default_args=default_args,
    catchup=False
) as dag:
    trigs = [
        TriggerDagRunOperator(
            task_id=f"trigger_dependent_dag_{x}",
            trigger_dag_id="dependent",
            wait_for_completion=True,
            execution_date=f"{{{{ data_interval_end.add(hours={x} * 2) }}}}",
            conf={"date": f"{{{{ data_interval_end.to_date_string() ~ ' 21:{x}0:00' }}}}"},
        )
    for x in range(4)]
    trigs


with DAG(
        "trigger-test",
        start_date=datetime(2023, 1, 10),
        max_active_runs=1,
        schedule_interval="* * * * *",
        default_args=default_args,
        catchup=False
) as dag:
    task = BashOperator(
        task_id='task',
        bash_command="echo date interval end: {{ data_interval_end.to_datetime_string() }}; echo tz: {{ data_interval_end.tz }}",
    )
