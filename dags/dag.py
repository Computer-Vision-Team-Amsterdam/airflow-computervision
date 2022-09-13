import os
import json
import shutil
from typing import Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth

from datetime import datetime, timedelta

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from pathlib import Path
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from common import MessageOperator

BASE_URL = f"https://3206eec333a04cc980799f75a593505a.objectstore.eu/intermediate/"

airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
KVUri = airflow_secrets["vault_url"]

#credential = DefaultAzureCredential()
client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
print(f"user MI is {client_id}")
credential = ManagedIdentityCredential(client_id=client_id)

client = SecretClient(vault_url=KVUri, credential=credential)


username_secret = client.get_secret(name="CloudVpsRawUsername")
password_secret = client.get_secret(name="CloudVpsRawPassword")

USERNAME = username_secret.value
PASSWORD = password_secret.value

container_vars = {
    "AZURE_CLIENT_ID": os.getenv("AZURE_CLIENT_ID"),
    "AZURE_TENANT_ID": os.getenv("AZURE_TENANT_ID"),
    "AIRFLOW__SECRETS__BACKEND": os.getenv("AIRFLOW__SECRETS__BACKEND"),
    "AIRFLOW__SECRETS__BACKEND_KWARGS": os.getenv("AIRFLOW__SECRETS__BACKEND_KWARGS"),
    "USER_ASSIGNED_MANAGED_IDENTITY": os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
    }


def pull_function(**kwargs):
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='push_task')
    print(ls)


def push_function(**kwargs):
    ls = ['a', 'b', 'c']
    return ls


def split_pano_id(pano_id: str) -> Tuple[str, str]:
    """
    Splits name of the panorama in TMX* and pano*
    """
    id_name = pano_id.split("_")[0]
    index = pano_id.index("_")
    img_name = pano_id[index + 1:]
    return id_name, img_name


def download_panorama_from_cloudvps(
        date: datetime, panorama_id: str, output_dir: Path = Path(".")
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
        print(f"python KVUri is {KVUri}")

    except Exception as e:
        print(f"Failed for panorama {panorama_id}:\n{e}")


def test_connection_python():
    for k, v in os.environ.items():
        print(f'{k}={v}')

    pano_dates = [datetime(2016, 3, 17), datetime(2016, 3, 17), datetime(2016, 3, 17)]
    pano_ids = [
        "TMX7315120208-000020_pano_0000_000000",
        "TMX7315120208-000020_pano_0000_000001",
        "TMX7315120208-000020_pano_0000_000002",
    ]

    for pano_date, pano_id in zip(pano_dates, pano_ids):
        download_panorama_from_cloudvps(pano_date, pano_id)


with DAG(
        "test",
        default_args={
            "depends_on_past": False,
            "email": ["CVT@amsterdam.nl"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="A test DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 9),
        catchup=False,
        tags=["test"],
) as dag:
    """
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )
   
    """
    test_connection_python = PythonOperator(
        task_id="test_connection_python",
        python_callable=test_connection_python
    )

    test = KubernetesPodOperator(
        name="test",
        task_id="test",
        image="cvtweuacrogidgmnhwma3zq.azurecr.io/test:latest",
        image_pull_policy="Always",
        env_vars=container_vars,
        hostnetwork=True,
        in_cluster=True,
        cmds=["python"],
        arguments=["test.py"],
        namespace="airflow-cvision2",
        get_logs=True
    )

    write_xcom = KubernetesPodOperator(
        namespace='airflow-cvision2',
        image='cvtweuacrogidgmnhwma3zq.azurecr.io/test:latest',
        cmds=["sh", "-c", "echo 'test_xcom' > /airflow/xcom/return.json"],
        name="write-xcom",
        hostnetwork=True,
        do_xcom_push=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="write-xcom",
        get_logs=True,
    )

    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"",
        task_id="pod_task_xcom_result",
    )


    """
    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_function,
        provide_context=True)

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function,
        provide_context=True)
    
    """
    xcom = write_xcom >> pod_task_xcom_result
    var_0 = test_connection_python
    var = test


    # volume_mount = k8s_models.V1VolumeMount(
    #     name="dags-pv",
    #     mount_path="/tmp",
    #     sub_path=None,
    #     read_only=False,
    # )
    #
    # volume = k8s_models.V1Volume(
    #     name="dags-pv",
    #     persistent_volume_claim=k8s_models.V1PersistentVolumeClaimVolumeSource(claim_name="dags-pvc"),
    # )
    #
    # k8s_io = KubernetesPodOperator(
    #     name="my-k8s-io-task",
    #     task_id="kubernetes-io",
    #     image="debian",
    #     cmds=["bash", "-cx"],
    #     arguments=["echo Hello from Debian on $(date)... on K8S! > /tmp/test"],
    #     namespace="airflow",
    #     get_logs=True,
    #     volumes=[volume],
    #     volume_mounts=[volume_mount]
    # )
    #
    # print_message = BashOperator(
    #     task_id="print_message",
    #     bash_command="cat /tmp/test"
    # )
    #
    # chain(k8s, k8s_io, print_message)
