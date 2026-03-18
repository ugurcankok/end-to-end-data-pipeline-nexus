import sys
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

default_args = {
    'owner': 'ugurcankok',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nexuslake_transformation_v1',
    default_args=default_args,
    description='Daily Bronze to Gold Spark Pipeline',
    schedule_interval='0 17 * * *', # Triggers every day at 5:00 PM
    catchup=False,
    tags=['nexuslake', 'spark', 'gold', 'silver']
) as dag:

    # 1. Task: Kafka -> Bronze (Incremental Ingest)
    bronze_task = SparkSubmitOperator(
        task_id='run_bronze_ingest',
        application='/opt/airflow/dags/src/processing/bronze_transform.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4',
        verbose=True
    )

    # 2. Task: Bronze -> Silver (Avro Parse & Cast)
    silver_task = SparkSubmitOperator(
        task_id='run_silver_transform',
        application='/opt/airflow/dags/src/processing/silver_transform.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4',
        verbose=True
    )

    # 3. Task: Silver -> Gold (Analytics & Athena Compliance)
    gold_task = SparkSubmitOperator(
        task_id='run_gold_transform',
        application='/opt/airflow/dags/src/processing/gold_transform.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4',
        verbose=True
    )

    bronze_task >> silver_task >> gold_task