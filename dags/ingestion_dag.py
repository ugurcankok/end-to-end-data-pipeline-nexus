import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Adding a path so that Airflow can locate the src folder
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.ingestion.producer import run_producer

default_args = {
    'owner': 'ugurcankok',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'nexuslake_ingestion_v1',
    default_args=default_args,
    description='Idempotent Ingestion to Kafka',
    schedule_interval='*/5 * * * *', # Every 5 minutes
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='produce_transactions_to_kafka',
        python_callable=run_producer
    )