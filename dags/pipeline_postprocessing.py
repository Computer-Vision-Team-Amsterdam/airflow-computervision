import os
from datetime import timedelta
from typing import Final
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

from azure.storage.blob import BlobServiceClient
from azure.identity import ManagedIdentityCredential

from environment import (
    BLOB_URL,
    POSTPROCESSING_CONTAINER_IMAGE,
)

# [registry]/[imagename]:[tag]
DATE = '{{dag_run.conf["date"]}}'  # set in config when triggering DAG

# Command that you want to run on container start
DAG_ID: Final = "cvt-pipeline-postprocessing"
DATATEAM_OWNER: Final = "cvision2"
DAG_LABEL: Final = {"team_name": DATATEAM_OWNER}
AKS_NAMESPACE: Final = os.getenv("AIRFLOW__KUBERNETES__NAMESPACE")
AKS_NODE_POOL: Final = "cvision2work"

# List here all environment variables that also needs to be
# used inside the K8PodOperator pod.
GENERIC_VARS_NAMES: list = [
    "USER_ASSIGNED_MANAGED_IDENTITY",
    "AIRFLOW__SECRETS__BACKEND_KWARGS",
    "AIRFLOW_CONN_POSTGRES_DEFAULT"
]


client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
credential = ManagedIdentityCredential(client_id=client_id)
blob_service_client = BlobServiceClient(account_url=BLOB_URL,
                                       credential=credential)


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

with DAG(
        DAG_ID,
        description="test-dag",
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
            'start_date': days_ago(1),
        },
        schedule_interval=None,
        template_searchpath=["/"],
        catchup=False,
) as dag:
    postprocessing = KubernetesPodOperator(
        task_id='postprocessing',
        namespace=AKS_NAMESPACE,
        image=POSTPROCESSING_CONTAINER_IMAGE,
        env_vars=get_generic_vars(),
        cmds=["python"],
        arguments=["/app/postprocessing.py",
                   "--date", DATE],
        labels=DAG_LABEL,
        name=DAG_ID,
        image_pull_policy="Always",
        get_logs=True,
        in_cluster=True,  # if true uses our service account token as aviable in Airflow on K8
        is_delete_operator_pod=True,  # if true delete pod when pod reaches its final state.
        log_events_on_failure=True,  # if true log the pod’s events if a failure occurs
        hostnetwork=True,  # If True enable host networking on the pod. Beware, this value must be
        # set to true if you want to make use of the pod-identity facilities like managed identity.
        reattach_on_restart=True,
        dag=dag,
        startup_timeout_seconds=3600,
        execution_timeout=timedelta(hours=4),
        node_selector={"nodetype": AKS_NODE_POOL},
        volumes=[],
        volume_mounts=[],
    )

    # FLOW

    flow = postprocessing
