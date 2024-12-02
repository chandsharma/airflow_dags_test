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
    'test_kubernetes_pod_operator',
    default_args=default_args,
    description='Test KubernetesPodOperator',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

test_task = KubernetesPodOperator(
    task_id='test_pod',
    name='test-pod',
    namespace='default',
    image='bitnami/kubectl:latest',
    cmds=['/bin/sh', '-c'],
    arguments=['echo "Hello from KubernetesPodOperator!"'],
    get_logs=True,
    dag=dag,
)
