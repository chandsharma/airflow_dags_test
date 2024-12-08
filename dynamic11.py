"""
This DAG demonstrates submitting a Spark application to Kubernetes using SparkKubernetesOperator
with a publicly accessible YAML file.
"""

from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_pi_public_yaml',
    default_args=default_args,
    description='Submit Spark job to Kubernetes with a public YAML file',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'spark', 'kubernetes'],
)

# Submit the Spark job using the public YAML file
submit_spark_job = SparkKubernetesOperator(
    task_id='submit_spark_pi_job',
    namespace='default',  # Replace with your Kubernetes namespace
    application_file='https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml',  # Public URL
    kubernetes_conn_id='kubernetes_default',  # Kubernetes connection in Airflow
    do_xcom_push=True,  # Push logs and metadata to XCom for later tasks
    dag=dag,
)

# Monitor the Spark job status
monitor_spark_job = SparkKubernetesSensor(
    task_id='monitor_spark_pi_job',
    namespace='default',
    application_name='spark-history-airflow-spark-dynamic',  # Update to match the name in your YAML file
    kubernetes_conn_id='kubernetes_default',
    poke_interval=60,  # Check every minute
    timeout=3600,  # Timeout after 1 hour
    dag=dag,
)

# Define task dependencies
submit_spark_job >> monitor_spark_job
