# Example taken from:
# https://dev.azure.com/CloudCompetenceCenter/Dataverwerking/_git/airflow-examples
#

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from common import default_args, MessageOperator

team_name = "dave"
workload_name = "example-normal-dag"
dag_id = team_name + "_" + workload_name


def print_hello():
    return "Hello world from first Airflow DAG!"


with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    description="Hello World DAG",
    schedule_interval="0 12 * * *",
) as dag:
    slack_at_start = MessageOperator(task_id="slack_at_start")

    task1 = BashOperator(
        task_id="bash_echo_message",
        bash_command= "echo 'This command could be adapted to do something more interesting than sending a message.'",
    )

    task2 = PythonOperator(task_id="hello_task", python_callable=print_hello)

(slack_at_start >> task1 >> task2)

