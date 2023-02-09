import socket
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


def test():
   sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   result = sock.connect_ex(('10.224.197.73',5432))
   if result == 0:
      print("Port is open")
   else:
      print("Port is not open")
   sock.close()


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
        max_active_runs=1,
        schedule_interval="*/5 * * * *",
        default_args=default_args,
        catchup=False
) as dag:
   PythonOperator(
      task_id="test_port",
      python_callable=test,
      dag=dag
   )
