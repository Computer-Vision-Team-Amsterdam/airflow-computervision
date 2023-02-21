import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


def test():
   response = requests.get("https://api.data.amsterdam.nl/panorama/panoramas/?limit_results=1")
   try:
      response.raise_for_status()
   except Exception as e:
      print(e)
      raise e
   print(response.content)


default_args = {
   'depends_on_past': False,
   'email': ['airflow@example.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 0,
   'retry_delay': timedelta(minutes=5),
}

with DAG(
        "test_port",
        start_date=datetime(2023, 1, 1),
        max_active_runs=5,
        schedule_interval="*/2 * * * *",
        default_args=default_args,
        catchup=False
) as dag:
   PythonOperator(
      task_id="test_port",
      python_callable=test,
      dag=dag
   )
