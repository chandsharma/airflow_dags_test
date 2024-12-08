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
    'spark_pi_f',
    default_args=default_args,
    description='Submit Spark job to Kubernetes with a public YAML file',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Submit the Spark job using the public YAML file
t1 = SparkKubernetesOperator(
    task_id='spark_pi_f_submit',
    namespace='default',  # Replace with your Kubernetes namespace
    application_file='https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml',  # Public URL
    kubernetes_conn_id='kubernetes_default',  # Kubernetes connection in Airflow
    do_xcom_push=True,  # Push logs and metadata to XCom for later tasks
    dag=dag,
)

# Monitor the Spark job status
t2 = SparkKubernetesSensor(
    task_id='spark_pi_f_monitor',
    namespace='default',
    application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_f_submit')['metadata']['name'] }}",  # Update to match the name in your YAML file
    kubernetes_conn_id='kubernetes_default',
    dag=dag,
)

# Define task dependencies
t1 >> t2
