import os
import json
from datetime import datetime, timedelta

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from common import MessageOperator


def authenticate_env_1():

    airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
    KVUri = airflow_secrets["vault_url"]

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)

    username_secret = client.get_secret(name="CloudVpsRawUsername")
    print(f"USERNAME: {username_secret.value}")



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

    authenticate_env_1 = PythonOperator(
        task_id="authenticate_env_1",
        python_callable=authenticate_env_1
    )


    retrieve_images = KubernetesPodOperator(
        name="test-cloudvps-connection",
        task_id="test_cloudvps_connection",
        image="cvtweuacrogidgmnhwma3zq.azurecr.io/retrieve-images:latest",
        hostnetwork=True,
        cmds=["python"],
        arguments=["retrieve_images.py"],
        namespace="airflow-cvision2",
        get_logs=True
    )

    var_1 = authenticate_env_1
    var_3 = retrieve_images

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
