from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    "air_dynamic1",
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow",
    schedule_interval=None,  # Trigger manually or set a desired schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't backfill
)

# Submit Spark job task
submit_spark_job = SparkKubernetesOperator(
    task_id="submit_air_dynamic1",
    name="submit-air-dynamic1",
    namespace="default",
    image="bitnami/kubectl:latest",
    application_file="https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml",
    get_logs=False,  # Logs won't be captured by the operator
    delete_on_termination=False,  # The Spark application won't be deleted after termination
    dag=dag,  # Attach the task to the DAG
)
