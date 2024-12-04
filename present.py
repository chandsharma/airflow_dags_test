from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "submit_spark_job_airflow_present",
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = KubernetesPodOperator(
    task_id="submit_spark_airflow_present",
    name="submit-spark-airflowm_present",
    namespace="default",
    image="bitnami/kubectl:latest",
    cmds=["kubectl", "apply", "-f", "https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml"],
    get_logs=False,
    is_delete_operator_pod=True,
    dag=dag,
)
