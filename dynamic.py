from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "submit_spark_job_airflow_spark_k8s2dynamic",
    default_args=default_args,
    description="Submit Spark job to Kubernetes via SparkKubernetesOperator",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Submit the Spark job using SparkKubernetesOperator
submit_spark_job = SparkKubernetesOperator(
    task_id="submit_spark_jobdynamic",
    namespace="default",
    application_file="https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml",  # Dynamic configuration
    kubernetes_conn_id="kubernetes_default",  # Connection to the Kubernetes cluster
    do_xcom_push=True,  # Capture application logs
    dag=dag,
)

# Monitor the Spark job status using SparkKubernetesSensor
monitor_spark_job = SparkKubernetesSensor(
    task_id="monitor_spark_jobdynamic",
    namespace="default",
    application_name="spark-history-airflow-spark-k8sdynamic",
    kubernetes_conn_id="kubernetes_default",
    poke_interval=60,  # Check every minute
    timeout=3600,  # Timeout after 1 hour
    dag=dag,
)

submit_spark_job >> monitor_spark_job
