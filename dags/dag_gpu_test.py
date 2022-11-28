from datetime import timedelta
from typing import Final, Optional
import os
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes import client

IMAGE: Optional[str] = 'cvtweuacrogidgmnhwma3zq.azurecr.io/gpu_test:latest'

DAG_ID: Final = "sia"
DATATEAM_OWNER: Final = "cvision2"
DAG_LABEL: Final = {"team_name": DATATEAM_OWNER}
AKS_NAMESPACE: Final = os.getenv("AIRFLOW__KUBERNETES__NAMESPACE")
AKS_NODE_POOL: Final = "cvision2work"
DATE = '{{dag_run.conf["date"]}}'  # set in config when triggering DAG

# List here all environment variables that also needs to be
# used inside the K8PodOperator pod.
GENERIC_VARS_NAMES: list = [
    "USER_ASSIGNED_MANAGED_IDENTITY",
    "AIRFLOW__SECRETS__BACKEND_KWARGS",
]
gpu_resources = client.V1ResourceRequirements(
    requests={
        "cpu": 1,
        "memory": "1Gi",
        "ephemeral-storage": "2Gi",
    },
    limits={"cpu": 2, "memory": "2Gi", "ephemeral-storage": "4Gi", "nvidia.com/gpu": 1},
)

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
    description="Dag to check individual containers before adding them into the pipeline",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': days_ago(1),
        'schedule_interval': None,
    },
    template_searchpath=["/"],
    catchup=False,
) as dag:
    test_gpu = KubernetesPodOperator(
        task_id='test_gpu',
        namespace=AKS_NAMESPACE,
        image=IMAGE,
        env_vars=get_generic_vars(),
        cmds=["python"],
        arguments=["/app/gpu_test.py"],
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
        # node_selector={"nodetype": AKS_NODE_POOL},
        resources=gpu_resources,
        volumes=[],
        volume_mounts=[],
    )

# FLOW
var = (
        test_gpu
)