from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
# from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "air_dynamic",
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow"
)

submit_spark_job = SparkKubernetesOperator(
    task_id="submit_air_dynamic",
    name="submit-air-dynamic",
    namespace="default",
    image="bitnami/kubectl:latest",
    application_file="https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml",
    get_logs=False,
    dag=dag,
)
