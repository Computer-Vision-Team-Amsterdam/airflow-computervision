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

airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
KVUri = airflow_secrets["vault_url"]

client = SecretClient(vault_url=KVUri, credential=credential)
# sia_token = client.get_secret(name="sia-token")
socket.setdefaulttimeout(100)

sia_token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJXNTRsQzhsei1Yek9tVDhOMDFCTXdXLXlYbTFodGpHRlBRR0FUQm1SNU9BIn0.eyJleHAiOjE2NjQyODc2MDAsImlhdCI6MTY2NDI4NjcwMCwianRpIjoiODgxZDI3ODktYzhkNS00OTRiLThhYWUtM2Y5MzZiOGQwYTFlIiwiaXNzIjoiaHR0cHM6Ly9pYW0uYW1zdGVyZGFtLm5sL2F1dGgvcmVhbG1zL2RhdGFwdW50LWFkLWFjYyIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI3YTY4YTdlYi1hOWM3LTRlNTctYmExNS01OWNkMDUxYWRjMDQiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJzaWEtY3Z0IiwiYWNyIjoiMSIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWRhdGFwdW50LWFkLWFjYyIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImNsaWVudElkIjoic2lhLWN2dCIsImNsaWVudEhvc3QiOiIxMC4yNDAuMjAyLjE4NSIsInByZWZlcnJlZF91c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1zaWEtY3Z0IiwiY2xpZW50QWRkcmVzcyI6IjEwLjI0MC4yMDIuMTg1IiwiZW1haWwiOiJjdnRAYW1zdGVyZGFtLm5sIn0.NXaTYUrEy2wr7vOkj9qgiYhou9C-aI_La7hVYjPMbjbnGNBY_es0IR8-2smATpwYAausx6eS8ZL8sjprB_eSH6lFr4RK6CQF4AOUwMBvapJVZzj84O9SPjWj3X6XkJCro-vLxjGNvpTvAtL-tliwjoyADUI8fkswzqATO82OXEiMu57upv1TjaWzx0neLAu-uLEUpBNxPm2EaWO1zthC83ZFUDNHzdtCvxfWdkbjwdlBHdElm-Rg0j0MgKUBsIZjeQWd4_-KTd8KbeRWQXUI5dymvcLhU0AiJJQBnHN7-g7Rucjtv561W09My1nBfgVoPyVR8_YuPsc3qTupm-dEag"

def check_sia_connection():
    url = "https://acc.api.data.amsterdam.nl/signals/v1/private/signals"
    headers = {'Authorization': "Bearer {}".format(sia_token)}
    response = requests.get(url, headers=headers)
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
