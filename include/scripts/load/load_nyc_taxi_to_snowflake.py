# %%
import os
import requests
from tempfile import NamedTemporaryFile
from snowflake.connector import connect
from dotenv import load_dotenv
from loguru import logger

# %%

load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE")
SCHEMA = "RAW"

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
        schema=SCHEMA,
    )

def download_nyc_taxi_from_cdn(year: int, month: int) -> str:
    """
    Download the NYC taxi trip Parquet file from CloudFront via streaming and save it locally as a temporary file.

    Args:
        year (int): Year of the taxi trip data to download.
        month (int): Month of the taxi trip data to download.

    Returns:
        str: Absolute file path to the temporary local Parquet file.
    """
    
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    
    logger.info(f"Downloading data from CloudFront: {url}")
    response = requests.get(url, stream=True, timeout=60)
    
    if response.status_code != 200:
        error_msg = f"Failed to access CloudFront. Status Code: {response.status_code}. URL: {url}"
        logger.error(error_msg)
        raise Exception(error_msg)

    with NamedTemporaryFile(
        suffix=".parquet",
        delete=False
        ) as temp_file:
        
        temp_path = temp_file.name
        
        for chunk in response.iter_content(chunk_size=8192):
            temp_file.write(chunk)
            
    logger.info(f"Temporary Parquet file saved locally at: {temp_path}")
    return temp_path

def upload_file_to_stage(cursor, local_path: str, file_name: str) -> None:
    """
    Upload a local file to the Snowflake internal stage using the PUT command.

    Args:
        cursor: Active database cursor for executing queries.
        local_path (str): Local path of the file to be uploaded.
        file_name (str): Target filename inside the Snowflake internal stage.

    Returns:
        None: Executes the PUT command directly on Snowflake.
    """
    
    logger.info(f"Uploading file {file_name} to Snowflake Internal Stage")
    cursor.execute(f"""
        PUT file://{local_path} 
        @NYC_TAXI_DB.RAW.NYC_TAXI_INTERNAL_STAGE/{file_name} 
        AUTO_COMPRESS=TRUE 
        OVERWRITE=TRUE;
    """)
    logger.info("Upload to stage completed successfully")

def copy_stage_to_raw_table(cursor, file_name: str, target_table: str = "RAW_TAXI_TRIPS"):
    """
    Execute the COPY INTO command to load data from the Snowflake internal stage into the target raw table.

    Args:
        cursor (snowflake.connector.cursor.SnowflakeCursor): Active database cursor for executing queries.
        file_name (str): Name of the file stored in the stage to be ingested.
        target_table (str, optional): Name of the raw destination table. Defaults to "RAW_TAXI_TRIPS".

    Returns:
        None: Executes the COPY INTO command directly on Snowflake.
    """
    
    logger.info(f"Loading data from stage into table {target_table}")
    cursor.execute(f"""
        COPY INTO NYC_TAXI_DB.RAW.{target_table}
        FROM @NYC_TAXI_DB.RAW.NYC_TAXI_INTERNAL_STAGE/{file_name}
        FILE_FORMAT = (FORMAT_NAME = 'NYC_TAXI_DB.RAW.PARQUET_FORMAT')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """)
    logger.info(f"Data successfully copied into table {target_table}")

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
        raise e

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