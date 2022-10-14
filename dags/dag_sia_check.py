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

BASE_URL = "https://acc.api.data.amsterdam.nl/signals/v1/private/signals"

client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
credential = ManagedIdentityCredential(client_id=client_id)

airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
KVUri = airflow_secrets["vault_url"]

client = SecretClient(vault_url=KVUri, credential=credential)
sia_password = client.get_secret(name="sia-password-acc")
socket.setdefaulttimeout(100)

def to_signal(text: str, date_now, lat_lng: dict):
    return {
        "text": text,
        "location": {
            "geometrie": {
                "type": "Point",
                "coordinates": [lat_lng["lng"], lat_lng["lat"]]
            }
        },
        "category": {
            "sub_category": "/signals/v1/public/terms/categories/overig/sub_categories/overig"
        },
        "reporter": {
            "email": "cvt@amsterdam.nl"
        },
        "incident_date_start": date_now.strftime("%Y-%m-%d %H:%M")
    }

def _get_access_token(client_id, client_secret):
    token_url = 'https://iam.amsterdam.nl/auth/realms/datapunt-ad-acc/protocol/openid-connect/token'
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        print("The server successfully answered the request.")
        return response.json()["access_token"]
    else:
        response.raise_for_status()

def _get_signals_page(access_token, page):
    if access_token is None:
        raise Exception("Access token cannot be None")

    headers = {'Authorization': "Bearer {}".format(access_token)}
    url = BASE_URL + page
    response = requests.get(BASE_URL, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        return response.raise_for_status()

def _post_signal(access_token):
    if access_token is None:
        raise Exception("Access token cannot be None")

    headers = {'Authorization': "Bearer {}".format(access_token)}

    text = "CVT Dit is een automatisch gegenereerd signaal."
    date_now: datetime = datetime.now()
    lat_lng = {"lat": 52.367527, "lng": 4.901257}  # TODO

    response = requests.post(
        BASE_URL,
        json=to_signal(text, date_now, lat_lng),
        headers=headers
    )

    if response.status_code == 200:
        return response.json()
    else:
        return response.raise_for_status()

def check_sia_connection():
    access_token = _get_access_token("sia-cvt", f"{sia_password.value}")
    print(_get_signals_page(access_token, "?page_size=1"))

    print(_post_signal(access_token))


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