import os
from datetime import timedelta, datetime
from typing import Final

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient

from slack_hooks.slack import (
    on_failure_callback,
    on_success_callback
)

from environment import (
    BLOB_URL,
)

DATE = '{{dag_run.conf["date"]}}'  # set in config when triggering DAG

# Command that you want to run on container start
DAG_ID: Final = "cvt-retrieve_images"
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


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        "trigger-multiprocessing-small",
        start_date=datetime(2023, 1, 1),
        max_active_runs=1,
        schedule_interval="0 20 * * 3",  # 20:00 UTC, will start every Wednesday at 21:00 CET
        default_args=default_args,
        catchup=False
) as dag:
    trigs = [
        TriggerDagRunOperator(
            task_id=f"trigger_dependent_dag_{x}",
            trigger_dag_id=DAG_ID,
            wait_for_completion=True,
            # creates (4) DAGRuns for the triggered DAG at 2 hour intervals from (and including) the current DAG's start time
            # data_interval_end is the moment at which the DAG is scheduled to start in UTC, i.e., 21:00 CET, converted to UTC
            execution_date=f"{{{{ data_interval_end.in_tz('Europe/Amsterdam').add(hours=1{x}) }}}}",  # data_interval_end is in UTC
            # execution_date=f"{{{{ data_interval_end.in_tz('Europe/Amsterdam').add(hours={x}) }}}}",  # data_interval_end is in UTC
            conf={"date": f"2022-12-31 21:0{x}:00.00"},
            # conf={"date": f"{{{{ data_interval_end.in_tz('Europe/Amsterdam').to_date_string() ~ ' 21:{x}0:00.00' }}}}"},
        )
        for x in range(10)]

with DAG(
        DAG_ID,
        start_date=datetime(2023, 1, 1),
        description="test-dag",
        default_args=default_args,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
        schedule_interval=None,
        template_searchpath=["/"],
        catchup=False,
) as dag:
    retrieve_images = KubernetesPodOperator(
        task_id='retrieve_images',
        namespace=AKS_NAMESPACE,
        image="cvtweuacrogidgmnhwma3zq.azurecr.io/retrieve_images:latest",
        # beware! If env vars are needed from worker,
        # add them here.
        env_vars=get_generic_vars(),
        cmds=["bash", "-c"],
        arguments=["/opt/retrieve_images.sh",
                   DATE],
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
        is_delete_operator_pod=True,  # if true delete pod when pod reaches its final state.
        log_events_on_failure=True,  # if true log the pod’s events if a failure occurs
        hostnetwork=True,  # If True enable host networking on the pod. Beware, this value must be
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

    # FLOW
    flow = retrieve_images
