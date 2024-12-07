from datetime import timedelta, datetime

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils.dates import days_ago
k8s_hook = KubernetesHook(conn_id='kubernetes_config')
# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1
}
# [END default_args]

# [START instantiate_dag]

dag = DAG(
    'spark_pii',
    start_date=days_ago(1),
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=['example']
)

submit = SparkKubernetesOperator(
    task_id='spark_transform_data',
    namespace='spark-operator',
    image="bitnami/kubectl:latest",
    application_file='https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml',
    kubernetes_conn_id='kubernetes_default',
    do_xcom_push=True,
)



submit
