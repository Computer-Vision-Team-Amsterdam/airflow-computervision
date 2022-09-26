import json
import os
from datetime import timedelta
import shutil
import socket
from datetime import datetime
from pathlib import Path
from typing import Final, Optional, Tuple
from airflow.utils.dates import days_ago

import requests
from requests.auth import HTTPBasicAuth
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Command that you want to run on container start
DAG_ID: Final = "sia"
DATATEAM_OWNER: Final = "cvision2"
DAG_LABEL: Final = {"team_name": DATATEAM_OWNER}
AKS_NAMESPACE: Final = os.getenv("AIRFLOW__KUBERNETES__NAMESPACE")
AKS_NODE_POOL: Final = "cvision2work"


client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
credential = ManagedIdentityCredential(client_id=client_id)
blob_service_client = BlobServiceClient(account_url="https://cvtdataweuogidgmnhwma3zq.blob.core.windows.net",
                                        credential=credential)

airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
KVUri = airflow_secrets["vault_url"]

client = SecretClient(vault_url=KVUri, credential=credential)
sia_password = client.get_secret(name="sia-password-acc")
socket.setdefaulttimeout(100)


def check_sia_connection():
    url = "https://acc.api.data.amsterdam.nl/signals/v1/private/users"
    response = requests.get(url, stream=True, auth=HTTPBasicAuth("sia-cvt", sia_password))
    print(f"Respose status code: {response.status_code}.")

with DAG(
    DAG_ID,
    description="Dag to check individual containers before adding them into the pipeline",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': days_ago(1),
    },
    template_searchpath=["/"],
    catchup=False,
) as dag:

    test_sia = PythonOperator(task_id='test_sia',
                              python_callable=check_sia_connection,
                              provide_context=True,
                              dag=dag)


# FLOW
var = (
        test_sia
)
