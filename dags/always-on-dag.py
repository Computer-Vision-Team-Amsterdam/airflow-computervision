import os
from datetime import timedelta
from typing import Final, Optional
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

# [registry]/[imagename]:[tag]
CONTAINER_IMAGE: Optional[str] = 'crdavebbn1ontweu01.azurecr.io/airflow-benk-iburgerzaken:test'

# Command that you want to run on container start
DAG_ID: Final = "test_dag_always_on"
DATATEAM_OWNER: Final = "cvision2"
DAG_LABEL: Final = {"team_name": DATATEAM_OWNER}
AKS_NAMESPACE: Final = os.getenv("AIRFLOW__KUBERNETES__NAMESPACE")
AKS_NODE_POOL: Final = "cvision2work"

# List here all environment variables that also needs to be
# used inside the K8PodOperator pod.
GENERIC_VARS_NAMES: list = [
    "USER_ASSIGNED_MANAGED_IDENTITY",
]

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


    # 2. Excuting containers
    # The KubernetesPodOperator enables you to run containerized workloads as
    # pods on Kubernetes from your DAG.
    # Each task will be executed by a dedicated pod.
    test_operator_always_on = KubernetesPodOperator(
            task_id='test-step-using-k8podoperator',
            namespace=AKS_NAMESPACE,
            image=CONTAINER_IMAGE,
            # beware! If env vars are needed from worker,
            # add them here.
            env_vars=get_generic_vars(),
            cmds=["/bin/bash", "-c"],
            arguments=["tail -f /dev/null"],
            labels=DAG_LABEL,
            name=DAG_ID,
            # Determines when to pull a fresh image, if 'IfNotPresent' will cause
            # the Kubelet to skip pulling an image if it already exists. If you
            # want to always pull a new image, set it to 'Always'.
            image_pull_policy="Always",
            # Known issue in the KubernetesPodOperator
            # https://stackoverflow.com/questions/55176707/airflow-worker-connection-broken-incompleteread0-bytes-read
            # set get_logs to false
            # If true, logs stdout output of container. Defaults to True.
            get_logs=True,
            in_cluster=True,  # if true uses our service account token as aviable in Airflow on K8
            is_delete_operator_pod=False,  # if true delete pod when pod reaches its final state.
            log_events_on_failure=True,  # if true log the pod’s events if a failure occurs
            hostnetwork=True,  # If True enable host networking on the pod. Be ware, this value must be
            # set to true if you want to make use of the pod-identity facilities like managed identity.
            reattach_on_restart=True,
            dag=dag,
            # Timeout to start up the Pod, default is 120.
            startup_timeout_seconds=3600,
            # to prevent tasks becoming marked as failed when taking longer
            # and deleting them if staling
            execution_timeout=timedelta(hours=4),
            # Select a specific nodepool to use. Could also be specified by nodeAffinity.
            node_selector={"nodetype": AKS_NODE_POOL},
            # List of Volume objects to pass to the Pod.
            volumes=[],
            # List of VolumeMount objects to pass to the Pod.
            volume_mounts=[],
        )
        # get containers that are the first (marked with `_1` as a posfix) that will also
        # add the header to the .csv file to be created


# FLOW
(
    test_operator_always_on
)




