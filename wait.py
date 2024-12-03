from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import subprocess

class SparkApplicationSensor(BaseSensorOperator):
    def poke(self, context):
        result = subprocess.run(
            ["kubectl", "get", "sparkapplication", "spark-history", "-o", "jsonpath='{.status.applicationState.state}'"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        state = result.stdout.decode('utf-8').strip().strip("'")
        return state == "COMPLETED"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'submit_spark_job11',
    default_args=default_args,
    description='Submit Spark job to Kubernetes',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = KubernetesPodOperator(
    task_id='submit_spark_job',
    name='submit-spark-pi',
    namespace='default',
    image='bitnami/kubectl:latest',
    cmds=['kubectl', 'apply', '-f', 'https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml'],
    get_logs=True,
    dag=dag,
)

wait_for_spark = SparkApplicationSensor(
    task_id="wait_for_spark",
    poke_interval=60,  # Check every minute
    timeout=3600,      # Timeout after 1 hour
    dag=dag,
)

submit_spark_job >> wait_for_spark
