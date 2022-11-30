from datetime import datetime
import os
import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from azure.keyvault.secrets import SecretClient
from azure.identity import ManagedIdentityCredential

client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
credential = ManagedIdentityCredential(client_id=client_id)

airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
KVUri = airflow_secrets["vault_url"]

client = SecretClient(vault_url=KVUri, credential=credential)
username = client.get_secret(name="airflow-variables-DecosUsername")
password = client.get_secret(name="airflow-variables-DecosPassword")
url = client.get_secret(name="airflow-variables-DecosURL")

def login_test(**context):
    values = {'username': username,
              'password': password}

    try:
        import requests
        r = requests.post(url, data=values)
        print(r.content)
    except Exception as e:
        print(e)

def login_test_twee(**context):
    try:
        import requests
        page = requests.get(url)
        print(page.text.encode('utf8'))
    except Exception as e:
        print(e)


with DAG(dag_id='bash_dag', schedule_interval=None, start_date=datetime(2022, 11, 24), catchup=False) as dag:
    telnet_test = BashOperator(
        task_id='bash_task',
        bash_command="echo -e '\x1dclose\x0d' | telnet sfte.amsterdam.nl 22",
        dag=dag
    )

    login_test = PythonOperator(
        task_id='login_test',
        python_callable=login_test,
        provide_context=True,
        dag=dag
    )

    login_test_twee = PythonOperator(
        task_id='login_test_twee',
        python_callable=login_test_twee,
        provide_context=True,
        dag=dag
    )

    flow = telnet_test >> login_test >> login_test_twee
