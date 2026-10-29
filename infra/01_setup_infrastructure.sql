-- Create the database and schemas
CREATE DATABASE IF NOT EXISTS NYC_TAXI_DB;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.RAW;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.MARTS;

-- Create virtual warehouses 
CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Create internal stage for raw data 
CREATE STAGE IF NOT EXISTS NYC_TAXI_DB.RAW.NYC_TAXI_INTERNAL_STAGE;

-- Create parquet and csv file formats for the stage
CREATE FILE FORMAT IF NOT EXISTS NYC_TAXI_DB.RAW.PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO';

CREATE FILE FORMAT IF NOT EXISTS NYC_TAXI_DB.RAW.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null');

-- Create tables in raw schema
CREATE TABLE IF NOT EXISTS NYC_TAXI_DB.RAW.RAW_TAXI_TRIPS (
    VendorID INT,
    tpep_pickup_datetime VARIANT,
    tpep_dropoff_datetime VARIANT,
    passenger_count INT,
    trip_distance FLOAT,
    RatecodeID INT,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT,
    cbd_congestion_fee FLOAT
);

CREATE OR REPLACE TABLE NYC_TAXI_DB.RAW.RAW_TAXI_ZONE_LOOKUP (
        LocationID INT,
        Borough STRING,
        Zone STRING,
        service_zone STRING
);