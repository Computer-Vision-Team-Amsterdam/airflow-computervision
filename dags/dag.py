from datetime import datetime, timedelta

from airflow import DAG
# from airflow.models.baseoperator import chain
# from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

# from kubernetes.client import models as k8s_models


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
    k8s = KubernetesPodOperator(
        name="my-k8s-task",
        task_id="kubernetes",
        image="debian",
        cmds=["bash", "-cx"],
        arguments=["echo Hello from Debian... on K8S!"],
        namespace="airflow",
        get_logs=True,
    )

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
