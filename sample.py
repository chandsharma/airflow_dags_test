from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sample',
    default_args=default_args,
    description='Submit Spark Pi job to Kubernetes',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = KubernetesPodOperator(
    task_id='submit_spark_job',
    name='submit-spark-pi',
    namespace='default',
    image='bitnami/kubectl:latest',  # This is for the kubectl tool
    cmds=['kubectl', 'apply', '-f', 'https://vishalsparklogs.blob.core.windows.net/spark-logs/yaml/sparktest8.yaml'],
    get_logs=True,
    dag=dag,
)

submit_spark_job
