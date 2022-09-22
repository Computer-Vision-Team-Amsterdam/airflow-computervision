# this dag has one python operator
# which does retrieval of images
# uploads them to storage account

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
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

from requests.auth import HTTPBasicAuth

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Command that you want to run on container start
DAG_ID: Final = "retrieval"
DATATEAM_OWNER: Final = "cvision2"
DAG_LABEL: Final = {"team_name": DATATEAM_OWNER}
AKS_NAMESPACE: Final = os.getenv("AIRFLOW__KUBERNETES__NAMESPACE")
AKS_NODE_POOL: Final = "cvision2work"

# List here all environment variables that also needs to be
# used inside the K8PodOperator pod.
GENERIC_VARS_NAMES: list = [
    "USER_ASSIGNED_MANAGED_IDENTITY",
    "AIRFLOW__SECRETS__BACKEND_KWARGS",
]

date = '{{dag_run.conf["date"]}}'

print(f"directory content: {os.listdir(os.getcwd())}")

client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
credential = ManagedIdentityCredential(client_id=client_id)
blob_service_client = BlobServiceClient(account_url="https://cvtdataweuogidgmnhwma3zq.blob.core.windows.net",
                                        credential=credential)

airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
KVUri = airflow_secrets["vault_url"]

client = SecretClient(vault_url=KVUri, credential=credential)
username_secret = client.get_secret(name="CloudVpsRawPassword")
password_secret = client.get_secret(name="CloudVpsRawPassword")
socket.setdefaulttimeout(100)

BASE_URL = f"https://3206eec333a04cc980799f75a593505a.objectstore.eu/intermediate/"
USERNAME = username_secret.value
PASSWORD = password_secret.value


def get_generic_vars() -> dict[str, str]:
    """Get generic environment variables all containers will need.

    Note: The K8PodOperator spins up a new node. This node needs
        to be fed with the nessacery env vars. Its not inheriting
        it from his big brother/sister/neutral the worker pod.

    :returns: All (generic) environment variables that need to be included into the container.
    """
    GENERIC_VARS_DICT: dict[str, str] = {
        variable: os.environ[variable] for variable in GENERIC_VARS_NAMES
    }
    return GENERIC_VARS_DICT


def split_pano_id(pano_id: str) -> Tuple[str, str]:
    """
    Splits name of the panorama in TMX* and pano*
    """
    id_name = pano_id.split("_")[0]
    index = pano_id.index("_")
    img_name = pano_id[index + 1:]
    return id_name, img_name


def download_panorama_from_cloudvps(
        date: datetime, panorama_id: str, output_dir: Path = Path("./retrieved_images")
) -> None:
    """
    Downloads panorama from cloudvps to local folder.
    """

    if Path(f"./{output_dir}/{panorama_id}.jpg").exists():
        print(f"Panorama {panorama_id} is already downloaded.")
        return
    id_name, img_name = split_pano_id(panorama_id)

    try:
        url = (
                BASE_URL
                + f"{date.year}/{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{id_name}/{img_name}.jpg"
        )

        response = requests.get(
            url, stream=True, auth=HTTPBasicAuth(USERNAME, PASSWORD)
        )
        if response.status_code == 404:
            raise FileNotFoundError(f"No resource found at {url}")
        filename = f"./{output_dir}/{panorama_id}.jpg"
        with open(filename, "wb") as out_file:
            shutil.copyfileobj(response.raw, out_file)
        del response

        print(f"{panorama_id} completed.")
    except Exception as e:
        print(f"Failed for panorama {panorama_id}:\n{e}")


def upload_to_storage_account() -> None:
    print(f'directory content in dags: {os.listdir(Path(os.getcwd(), "dags"))}')
    print(f'directory content in repo: {os.listdir(Path(os.getcwd(), "dags", "repo"))}')
    print(f'directory content in 25ed..: {os.listdir(Path(os.getcwd(), "dags", "25ed2ff3cea086b7b72795f15c865dc8092ac0ec"))}')

    retrieved_images_folder_path = Path(os.getcwd(), "dags", "retrieved_images")
    for file in os.listdir(retrieved_images_folder_path):
        blob_client = blob_service_client.get_blob_client(
            container="unblurred", blob=f"{date}/{file}")

        # Upload the created file
        with open(os.path.join(retrieved_images_folder_path, file), "rb") as data:
            blob_client.upload_blob(data)


def retrieve_function():
    pano_dates = [datetime(2016, 3, 17), datetime(2016, 3, 17), datetime(2016, 3, 17)]
    pano_ids = [
        "TMX7315120208-000020_pano_0000_000000",
        "TMX7315120208-000020_pano_0000_000001",
        "TMX7315120208-000020_pano_0000_000002",
    ]

    for pano_date, pano_id in zip(pano_dates, pano_ids):
        download_panorama_from_cloudvps(pano_date, pano_id)



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
    """
    retrieve = PythonOperator(task_id='retrieve', python_callable=retrieve_function, dag=DAG_ID)
    """
    upload = PythonOperator(task_id='upload',
                            python_callable=upload_to_storage_account,
                            provide_context=True,
                            dag=dag)

# FLOW
var = (
        upload
)
