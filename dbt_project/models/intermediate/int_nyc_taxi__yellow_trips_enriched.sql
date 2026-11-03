{{ config(
    materialized = 'view'
) }}

WITH base_staging AS (
    SELECT * FROM {{ ref('stg_nyc_taxi__yellow_trips') }}
),

calculated_timestamps AS (
    SELECT
        *,
        -- Generate surrogate keys for dates (Format YYYYMMDD)
        CAST(TO_CHAR(pickup_datetime, 'YYYYMMDD') AS INT) AS pickup_date_key,
        CAST(TO_CHAR(dropoff_datetime, 'YYYYMMDD') AS INT) AS dropoff_date_key,

        -- Generate temporal keys for hours (0-23)
        HOUR(pickup_datetime) AS pickup_time_key,
        HOUR(dropoff_datetime) AS dropoff_time_key,

        -- Base trip duration calculation in minutes
        DATEDIFF('second', pickup_datetime, dropoff_datetime)
        / 60.0 AS trip_duration_min
        
    FROM base_staging
),

calculated_metrics AS (
    SELECT
        *,
        -- Average speed calculation (MPH)
        CASE
            WHEN
                (DATEDIFF('second', pickup_datetime, dropoff_datetime) / 3600.0)
                > 0
                THEN
                    ROUND(
                        trip_distance
                        / (
                            DATEDIFF(
                                'second', pickup_datetime, dropoff_datetime
                            )
                            / 3600.0
                        ),
                        2
                    )
            ELSE 0.0
        END AS speed_mph,

        -- Financial and operational data audit flags
        (fare_amount < 0) AS is_adjustment,
        (trip_distance = 0) AS is_zero_distance
    FROM calculated_timestamps
),

evaluated_quality_rules AS (
    SELECT
        *,
        -- Isolation of anomaly and duration quality rules
        CASE
            WHEN trip_duration_min <= 0 THEN 'non_positive_duration'
            WHEN trip_duration_min > 180 THEN 'duration_over_180'
            WHEN
                trip_duration_min > 5
                AND (
                    (
                        trip_distance
                        / (
                            DATEDIFF(
                                'second', pickup_datetime, dropoff_datetime
                            )
                            / 3600.0
                        )
                    )
                    < 2
                )
                THEN 'implausible_speed'
            ELSE 'valid'
        END AS duration_issue
    FROM calculated_metrics
)

SELECT
    -- Source identifiers
    vendor_id,
    ratecode_id,
    payment_type,
    pickup_location_id,
    dropoff_location_id,

    -- Timestamps and temporal keys
    pickup_datetime,
    dropoff_datetime,
    pickup_date_key,
    dropoff_date_key,
    pickup_time_key,
    dropoff_time_key,

    -- Degenerate attributes, operational and financial metrics
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    trip_duration_min,
    speed_mph,

    -- Governance and quality
    duration_issue,
    is_adjustment,
    is_zero_distance,
    (duration_issue = 'valid') AS is_valid_duration

FROM evaluated_quality_rules
