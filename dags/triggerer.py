from airflow import DAG, XComArg
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
    my_format = "%Y-%m-%d_%H-%M-%S"
    first_run = datetime.combine(
        date=datetime.strptime(ctx["dag_run"].conf["date"], "%Y-%m-%d"),
        time=time(hour=21),
    )
    trigger_times = [first_run + timedelta(hours=x * 2) for x in range(n_runs)]
    arguments = [(first_run + timedelta(minutes=x)).strftime(my_format) for x in range(n_runs)]
    return arguments


with DAG(
    "trigger-dagrun-dag",
    start_date=datetime(2023, 1, 9),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:
    triggers = PythonOperator(
        task_id='generate-triggers',
        python_callable=trigger_generator,
        provide_context=True,
    )

    ops = TriggerDagRunOperator.partial(
        task_id="trigger_dependent_dag",
        trigger_dag_id="dependent",
        wait_for_completion=True
    ).expand(
        conf={"date": XComArg(triggers)},
    )

    ops
