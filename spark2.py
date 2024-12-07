from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
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
    "submit_spark_job_airflow_spark_k8s2",
    default_args=default_args,
    description="Submit Spark job to Kubernetes via SparkKubernetesOperator",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Submit the Spark job using SparkKubernetesOperator
submit_spark_job = SparkKubernetesOperator(
    task_id="submit_spark_job",
    namespace="default",
    application_file=None,  # Dynamic configuration
    spark_application={
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": "spark-history-airflow-spark-k8s2",
            "namespace": "default",
        },
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "image": "docker.io/channnuu/chandan_spark:3.5.2",
            "imagePullPolicy": "IfNotPresent",
            "mainClass": "org.apache.spark.examples.SparkPi",
            "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.2.jar",
            "sparkVersion": "3.1.2",
            "restartPolicy": {"type": "Never"},
            "sparkConf": {
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": "wasb://spark-logs@vishalsparklogs.blob.core.windows.net/spark-logs",
                "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
                "spark.hadoop.fs.azure.account.key.vishalsparklogs.blob.core.windows.net": "XZfQviaXeNqQVHjTD6Cwg1VbiUK8YhDWqOSTDskYv5oFd4YzfajqGUHZBE3/2My1mw9hPXfeceYn+AStsFBh7A==",
            },
            "driver": {
                "cores": 1,
                "memory": "512m",
                "serviceAccount": "spark",
                "labels": {"version": "3.1.2"},
                "env": [{"name": "SPARK_DRIVER_MEMORY", "value": "512m"}],
            },
            "executor": {
                "cores": 1,
                "instances": 2,
                "memory": "512m",
            },
        },
    },
    kubernetes_conn_id="kubernetes_default",  # Connection to the Kubernetes cluster
    do_xcom_push=True,  # Capture application logs
    dag=dag,
)

# Monitor the Spark job status using SparkKubernetesSensor
monitor_spark_job = SparkKubernetesSensor(
    task_id="monitor_spark_job2",
    namespace="default",
    application_name="spark-history-airflow-spark-k8s2",
    kubernetes_conn_id="kubernetes_default",
    poke_interval=60,  # Check every minute
    timeout=3600,  # Timeout after 1 hour
    dag=dag,
)

submit_spark_job >> monitor_spark_job
