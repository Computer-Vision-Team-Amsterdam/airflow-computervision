from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG(dag_id='bash_dag', schedule_interval=None, start_date=datetime(2022, 11, 24), catchup=False) as dag:
  BashOperator(task_id='bash_task', bash_command="echo -e '\x1dclose\x0d' | telnet sfte.amsterdam.nl 22")
 
