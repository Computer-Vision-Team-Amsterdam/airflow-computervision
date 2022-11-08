# Example taken from:
# https://dev.azure.com/CloudCompetenceCenter/Dataverwerking/_git/airflow-examples
#

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from common import default_args

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
    task1 = BashOperator(
        task_id="send_slack_message",
        bash_command= "echo 'This command could be adapted to send a message on Slack.'",
    )

    task2 = PythonOperator(task_id="hello_task", python_callable=print_hello)

(task1 >> task2)

