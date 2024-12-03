from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import subprocess

class SparkApplicationSensor(BaseSensorOperator):
    def poke(self, context):
        try:
            result = subprocess.run(
                ["kubectl", "get", "sparkapplication", "spark-history", "-o", "jsonpath={.status.applicationState.state}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            state = result.stdout.decode('utf-8').strip().strip("'")
            self.log.info(f"Current SparkApplication state: {state}")
            return state == "COMPLETED"
        except subprocess.CalledProcessError as e:
            self.log.error(f"Error fetching SparkApplication state: {e.stderr.decode('utf-8')}")
            return False

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'submit_spark_jairflow',
    default_args=default_args,
    description='Submit Spark job to Kubernetes',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = KubernetesPodOperator(
    task_id='submit_spark_airflow',
    name='submit-spark-airflow',
    namespace='default',
    image='bitnami/kubectl:latest',
    cmds=['kubectl', 'apply', '-f', 'https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml'],
    get_logs=True,
    dag=dag,
)

wait_for_spark = SparkApplicationSensor(
    task_id="wait_for_sparkairflow",
    poke_interval=60,  # Check every minute
    timeout=3600,      # Timeout after 1 hour
    dag=dag,
)

submit_spark_job >> wait_for_spark
