from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import subprocess

class SparkApplicationSensor(BaseSensorOperator):
    """
    Sensor to monitor the status of a SparkApplication.
    """
    def poke(self, context):
        try:
            # Get the current status of the SparkApplication
            result = subprocess.run(
                ["kubectl", "get", "sparkapplication", "spark-history-airflow6", "-o", "jsonpath={.status.applicationState.state}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            state = result.stdout.decode('utf-8').strip()
            self.log.info(f"Current SparkApplication state: {state}")
            
            # Fetch and log driver pod logs if SparkApplication is COMPLETED
            if state == "COMPLETED":
                self.log.info("SparkApplication completed. Fetching driver pod logs...")
                logs_result = subprocess.run(
                    ["kubectl", "logs", "-l", "spark-role=driver", "-n", "default"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True,
                )
                driver_logs = logs_result.stdout.decode('utf-8')
                self.log.info(f"Driver pod logs:\n{driver_logs}")
                return True
            
            # Log if the state is PENDING, RUNNING, etc.
            return False
        except subprocess.CalledProcessError as e:
            self.log.error(f"Error fetching SparkApplication state or logs: {e.stderr.decode('utf-8')}")
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
    'submit_spark_job_airflow6',
    default_args=default_args,
    description='Submit Spark job to Kubernetes via Airflow',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = KubernetesPodOperator(
    task_id='submit_spark_airflow6',
    name='submit-spark-airflow6',
    namespace='default',
    image='bitnami/kubectl:latest',
    cmds=['kubectl', 'apply', '-f', 'https://vishalsparklogs.blob.core.windows.net/spark-logs/sparktest8.yaml'],
    get_logs=False,
    is_delete_operator_pod=True,
    dag=dag,
)

wait_for_spark = SparkApplicationSensor(
    task_id="wait_for_spark_airflow6",
    poke_interval=60,  # Check every minute
    timeout=3600,      # Timeout after 1 hour
    dag=dag,
)

submit_spark_job >> wait_for_spark
