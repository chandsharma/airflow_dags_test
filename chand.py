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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    'spark_pi_job_public_yaml',
    default_args=default_args,
    description='Run Spark job using a public YAML file',
    schedule_interval=None,  # Run manually
    start_date=datetime(2024, 11, 30),
    catchup=False,
    tags=['example', 'spark', 'public_yaml'],
) as dag:

    # Submit the Spark job
    submit_spark_job = SparkKubernetesOperator(
        task_id='submit_spark_job',
        namespace='default',  # Replace with your Kubernetes namespace
        application_file='https://vishalsparklogs.blob.core.windows.net/spark-logs/yaml/sparktest8.yaml',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    # Monitor the Spark job
    monitor_spark_job = SparkKubernetesSensor(
        task_id='monitor_spark_job',
        namespace='default',  # Replace with your Kubernetes namespace
        application_name="{{ task_instance.xcom_pull(task_ids='submit_spark_job')['metadata']['name'] }}",
        kubernetes_conn_id='kubernetes_default',
        poke_interval=30,  # Check every 30 seconds
        timeout=600,  # Timeout after 10 minutes
    )

    # Task dependency
    submit_spark_job >> monitor_spark_job
