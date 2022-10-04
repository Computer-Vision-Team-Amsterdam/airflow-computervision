import os
from datetime import timedelta
from typing import Final, Optional
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

# [registry]/[imagename]:[tag]
DETECT_CONTAINER_IMAGE: Optional[str] = 'cvtweuacrogidgmnhwma3zq.azurecr.io/detection:latest'

# Command that you want to run on container start
DAG_ID: Final = "debugging-detection"
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

DATE = '{{dag_run.conf["date"]}}'  # set in config when triggering DAG


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
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': days_ago(1),
    },
    template_searchpath=["/"],
    catchup=False,
) as dag:
    detect_containers = KubernetesPodOperator(
        task_id='detect_containers',
        namespace=AKS_NAMESPACE,
        image=DETECT_CONTAINER_IMAGE,
        env_vars=get_generic_vars(),
        cmds=["python"],
        arguments=["/app/inference.py",
                   "--subset", DATE,
                   "--device", "cpu",
                   "--data_folder", "blurred",
                   "--weights", "model_final.pth",
                   "--output_path", "outputs"],
        labels=DAG_LABEL,
        name=DAG_ID,
        image_pull_policy="Always",
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=False,
        log_events_on_failure=True,
        hostnetwork=True,
        reattach_on_restart=True,
        dag=dag,
        startup_timeout_seconds=3600,
        execution_timeout=timedelta(hours=4),
        node_selector={"nodetype": AKS_NODE_POOL},
        volumes=[],
        volume_mounts=[],
    )

# FLOW
var = (
        detect_containers
)
