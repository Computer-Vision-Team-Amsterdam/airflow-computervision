from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common import default_args


def script():
    from kubernetes import client, config

    config.load_kube_config()

    v1 = client.CoreV1Api()
    print("Listing pods with their IPs:")
    ret = v1.list_pod_for_all_namespaces(watch=False)
    for i in ret.items:
        print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))


with DAG(
        "kubeapi",
        default_args=default_args,
        description="Test Kubeapi access",
        schedule_interval=None,
) as dag:
    task1 = PythonOperator(
        task_id="kubeapi",
        python_callable=script,
    )
