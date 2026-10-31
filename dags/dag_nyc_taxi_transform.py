import os
from datetime import datetime
from airflow import DAG
from airflow.datasets import Dataset
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

raw_taxi_dataset = Dataset("snowflake://nyc_taxi_db/raw/raw_taxi_trips")

# 2. Configuração do perfil do Snowflake para o Cosmos
profile_config = ProfileConfig(
    profile_name="nyc_taxi_insights_engine",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
            "database": "NYC_TAXI_DB",
            "schema": "STAGING",
        },
    ),
)

# 3. Caminho absoluto para a pasta do seu dbt_project
DBT_ROOT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../dbt_project")
)

# 4. A DAG do dbt controlada pelo Cosmos
my_dbt_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_ROOT_PATH,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/.venv/bin/dbt",
    ),
    dag_id="nyc_taxi_dbt_transformation_pipeline",
    schedule=[raw_taxi_dataset], 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)