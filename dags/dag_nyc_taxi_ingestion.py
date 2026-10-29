# dags/dag_nyc_taxi_ingestion.py

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from include.scripts.load.load_nyc_taxi_to_snowflake import run_pipeline

default_args = {
    'owner': 'fabio',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nyc_taxi_raw_ingestion_pipeline',
    default_args=default_args,
    description='Pipeline to ingest NYC Taxi Parquet data from CloudFront to Snowflake RAW layer',
    schedule_interval='0 3 5 * *',  # Roda todo dia 5 do mês às 03:00 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'raw', 'snowflake', 'cloudfront'],
) as dag:

    ingest_trips_task = PythonOperator(
        task_id='ingest_yellow_taxi_trips',
        python_callable=run_pipeline,
        op_kwargs={
            'year': 2026,
            'month': 1,
        }
    )

    ingest_trips_task