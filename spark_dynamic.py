from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.sensors.kubernetes_pod import KubernetesPodSensor
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
    "submit_spark_job_airflow_dynamic",
    default_args=default_args,
    description="Submit Spark job to Kubernetes by dynamically creating application",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define the Spark application as a multi-line string
spark_application = """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: spark-history-airflow-dynamic
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "docker.io/channnuu/chandan_spark:3.5.2"
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.2.jar"
  sparkVersion: "3.1.2"
  restartPolicy:
    type: Never
  sparkConf:
    "spark.eventLog.enabled": "true"
    "spark.eventLog.dir": "wasb://spark-logs@vishalsparklogs.blob.core.windows.net/spark-logs"
    "spark.kubernetes.authenticate.driver.serviceAccountName": "spark"
    "spark.hadoop.fs.azure.account.key.vishalsparklogs.blob.core.windows.net": "XZfQviaXeNqQVHjTD6Cwg1VbiUK8YhDWqOSTDskYv5oFd4YzfajqGUHZBE3/2My1mw9hPXfeceYn+AStsFBh7A=="
  driver:
    cores: 1
    memory: "512m"
    serviceAccount: spark
    labels:
      version: "3.1.2"
    env:
      - name: SPARK_DRIVER_MEMORY
        value: "512m"
  executor:
    cores: 1
    instances: 2
    memory: "512m"
"""

# Submit the Spark application using KubernetesPodOperator
submit_spark_job = KubernetesPodOperator(
    task_id="submit_spark_job_dynamic",
    namespace="default",
    name="submit-spark-job",
    image="bitnami/kubectl:latest",
    cmds=["/bin/sh", "-c"],
    arguments=[
        f"echo '{spark_application}' > /tmp/spark-application.yaml && kubectl apply -f /tmp/spark-application.yaml"
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=True,
    dag=dag,
)

# Monitor the Spark application using KubernetesPodSensor
monitor_spark_job = KubernetesPodSensor(
    task_id="monitor_spark_job_dynamic",
    namespace="default",
    pod_name="spark-history-airflow-dynamic-driver",
    kubernetes_conn_id="kubernetes_default",
    poke_interval=60,
    timeout=3600,
    dag=dag,
)

submit_spark_job >> monitor_spark_job
