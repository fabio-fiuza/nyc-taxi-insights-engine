# %%
import os
import requests
import tempfile
from snowflake.connector import connect
from dotenv import load_dotenv
from loguru import logger

# %%
load_dotenv()

REQUIRED_ENV_VARS = (
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_WAREHOUSE",
)

missing_env_vars = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
if missing_env_vars:
    raise RuntimeError(
        f"Missing required environment variables: {', '.join(missing_env_vars)}"
    )

SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_DATABASE = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]

RAW_SCHEMA = "RAW"
RAW_STAGE = "NYC_TAXI_INTERNAL_STAGE"
PARQUET_FORMAT = "PARQUET_FORMAT"
RAW_TABLE = "RAW_TAXI_TRIPS"

# %%
def get_snowflake_connection():
    """
    Establish and return an active connection to the Snowflake data warehouse.

    Returns:
        snowflake.connector.connection.SnowflakeConnection: Active connection object to Snowflake database.
    """
    logger.info("Connecting to Snowflake database...")
    return connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=RAW_SCHEMA,
    )

def download_nyc_taxi_from_cdn(year: int, month: int) -> str:
    """
    Download the NYC taxi trip Parquet file from CloudFront via streaming and save it locally with its deterministic name.

    Args:
        year (int): Year of the taxi trip data to download.
        month (int): Month of the taxi trip data to download.

    Returns:
        str: Absolute file path to the downloaded Parquet file.
    """
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    
    logger.info(f"Downloading data from CloudFront: {url}")
    response = requests.get(url, stream=True, timeout=60)
    
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        logger.error(
            "Failed to access CloudFront. Status Code: {}. URL: {}",
            response.status_code,
            url,
        )
        raise RuntimeError(
            f"Failed to download {file_name}: HTTP {response.status_code}"
        ) from exc

    target_path = os.path.join(tempfile.gettempdir(), file_name)
    with open(target_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            
    logger.info(f"Parquet file saved locally at: {target_path}")
    return target_path

def upload_file_to_stage(cursor, local_path: str, file_name: str) -> None:
    """
    Upload a local file to the Snowflake internal stage, removing stale files under the same prefix.

    Args:
        cursor: Active database cursor for executing queries.
        local_path (str): Local path of the file to be uploaded.
        file_name (str): Target filename inside the Snowflake internal stage.

    Returns:
        None: Executes clean stage removal and PUT on Snowflake.
    """
    logger.info(f"Cleaning stale stage files under prefix {file_name}...")
    cursor.execute(
        f"REMOVE @{SNOWFLAKE_DATABASE}.{RAW_SCHEMA}.{RAW_STAGE}/{file_name};"
    )

    logger.info(f"Uploading file {file_name} to Snowflake Internal Stage...")
    cursor.execute(f"""
        PUT file://{local_path} 
        @{SNOWFLAKE_DATABASE}.{RAW_SCHEMA}.{RAW_STAGE}/
        AUTO_COMPRESS=FALSE
        OVERWRITE=TRUE;
    """)
    logger.info("Upload to stage completed successfully")

def copy_stage_to_raw_table(
    cursor,
    file_name: str,
    target_table: str = RAW_TABLE,
) -> None:
    """
    Execute the COPY INTO command to load data from the Snowflake internal stage into the target raw table.
    Ensures idempotency by deleting any previously loaded data for the same source file before copying.

    Args:
        cursor (snowflake.connector.cursor.SnowflakeCursor): Active database cursor for executing queries.
        file_name (str): Name of the file stored in the stage to be ingested.
        target_table (str, optional): Name of the raw destination table. Defaults to "RAW_TAXI_TRIPS".

    Returns:
        None: Executes idempotency cleanup, COPY INTO, and metadata tracking on Snowflake.
    """
    try:
        cursor.execute("BEGIN;")

        logger.info(f"Ensuring target table schema has source_file_name column...")
        cursor.execute(f"""
            ALTER TABLE {SNOWFLAKE_DATABASE}.{RAW_SCHEMA}.{target_table}
            ADD COLUMN IF NOT EXISTS source_file_name STRING;
        """)

        logger.info(f"Removing existing records for file {file_name} to guarantee idempotency...")
        cursor.execute(
            f"""
            DELETE FROM {SNOWFLAKE_DATABASE}.{RAW_SCHEMA}.{target_table}
            WHERE source_file_name = %s;
            """,
            (file_name,),
        )

        logger.info(f"Loading data from stage into table {target_table}...")
        cursor.execute(f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{RAW_SCHEMA}.{target_table}
            FROM @{SNOWFLAKE_DATABASE}.{RAW_SCHEMA}.{RAW_STAGE}/{file_name}
            FILE_FORMAT = (
                FORMAT_NAME = '{SNOWFLAKE_DATABASE}.{RAW_SCHEMA}.{PARQUET_FORMAT}'
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """)

        logger.info(f"Tagging ingested records with source file name {file_name}...")
        cursor.execute(
            f"""
            UPDATE {SNOWFLAKE_DATABASE}.{RAW_SCHEMA}.{target_table}
            SET source_file_name = %s
            WHERE source_file_name IS NULL;
            """,
            (file_name,),
        )
        cursor.execute("COMMIT;")
        logger.info(f"Data successfully copied and tagged for file {file_name}")
    except Exception:
        cursor.execute("ROLLBACK;")
        logger.error(f"Transaction failed while loading file {file_name}; changes rolled back")
        raise

def run_pipeline(year: int = 2026, month: int = 1):
    """
    Orchestrate the ETL ingestion pipeline for NYC Taxi trips data from CloudFront.

    Args:
        year (int, optional): Year parameter for the trips data. Defaults to 2026.
        month (int, optional): Month parameter for the trips data. Defaults to 1.

    Returns:
        None: Runs the trips ingestion pipeline and handles resource cleanup.
    """
    
    logger.info(f"--- Starting ingestion pipeline for {year}-{month:02d} ---")
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    temp_parquet_path = None
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"

    try:
        temp_parquet_path = download_nyc_taxi_from_cdn(year, month)
        upload_file_to_stage(cursor, temp_parquet_path, file_name)
        copy_stage_to_raw_table(cursor, file_name, target_table="RAW_TAXI_TRIPS")
        logger.success("--- Pipeline executed successfully! ---")
        
    except Exception as e:
        logger.error(f"Error occurred during pipeline execution: {e}")
        raise

    finally:
        if temp_parquet_path and os.path.exists(temp_parquet_path):
            os.remove(temp_parquet_path)
            logger.info("Local temporary file cleaned up successfully")
            
        cursor.close()
        conn.close()
        logger.info("Snowflake connection closed")

# %%
if __name__ == "__main__":
    run_pipeline(year=2026, month=1)
    

# %%
