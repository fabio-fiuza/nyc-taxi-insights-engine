from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.datasets import Dataset 
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from include.scripts.load.load_nyc_taxi_to_snowflake import run_pipeline

# 2. Definimos o Dataset com o caminho exato que representa o dado pronto no Snowflake
raw_taxi_dataset = Dataset("snowflake://nyc_taxi_db/raw/raw_taxi_trips")

default_args = {
    "owner": "fabio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_raw_ingestion_pipeline",
    default_args=default_args,
    description="Pipeline to ingest NYC Taxi Parquet data from CloudFront to Snowflake RAW layer",
    schedule_interval="0 3 5 * *",  # Roda todo dia 5 do mês às 03:00 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nyc_taxi", "raw", "snowflake", "cloudfront"],
) as dag:

  ingest_trips_task = PythonOperator(
      task_id="ingest_yellow_taxi_trips",
      python_callable=run_pipeline,
      op_kwargs={
          "year": 2026,
          "month": 1,
      },
      outlets=[
          raw_taxi_dataset
      ], 
  )

  ingest_trips_task