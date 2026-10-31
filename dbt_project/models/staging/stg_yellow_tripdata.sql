{{
    config(
        materialized='view'
    )
}}

SELECT
    -- Identifiers
    VendorID AS vendor_id,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    RatecodeID AS ratecode_id,
    payment_type AS payment_type,

    -- Timestamps (Casting VARIANT to string first using CAST, then to timestamp)
    CAST(CAST(tpep_pickup_datetime AS VARCHAR) AS TIMESTAMP_NTZ) AS pickup_datetime,
    CAST(CAST(tpep_dropoff_datetime AS VARCHAR) AS TIMESTAMP_NTZ) AS dropoff_datetime,

    -- Operational Metrics
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS NUMERIC(10, 2)) AS trip_distance,
    store_and_fwd_flag AS store_and_fwd_flag,

    -- Financial Metrics
    CAST(fare_amount AS NUMERIC(10, 2)) AS fare_amount,
    CAST(extra AS NUMERIC(10, 2)) AS extra,
    CAST(mta_tax AS NUMERIC(10, 2)) AS mta_tax,
    CAST(tip_amount AS NUMERIC(10, 2)) AS tip_amount,
    CAST(tolls_amount AS NUMERIC(10, 2)) AS tolls_amount,
    CAST(improvement_surcharge AS NUMERIC(10, 2)) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC(10, 2)) AS total_amount,
    CAST(congestion_surcharge AS NUMERIC(10, 2)) AS congestion_surcharge,
    CAST(Airport_fee AS NUMERIC(10, 2)) AS airport_fee,
    CAST(cbd_congestion_fee AS NUMERIC(10, 2)) AS cbd_congestion_fee

FROM {{ source('nyc_tripdata', 'raw_taxi_trips') }}