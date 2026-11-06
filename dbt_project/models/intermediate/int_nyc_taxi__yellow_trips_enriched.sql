{{ config(materialized = 'view') }}

WITH base_staging AS (
    SELECT *
    FROM {{ ref('stg_nyc_taxi__yellow_trips') }}
),

calculated_timestamps AS (
    SELECT
        *,
        CAST(TO_CHAR(pickup_datetime, 'YYYYMMDD') AS INT) AS pickup_date_id,
        CAST(TO_CHAR(dropoff_datetime, 'YYYYMMDD') AS INT) AS dropoff_date_id,
        HOUR(pickup_datetime) AS pickup_time_id,
        HOUR(dropoff_datetime) AS dropoff_time_id,
        DATEDIFF('second', pickup_datetime, dropoff_datetime) / 60.0 AS trip_duration_min,
        (YEAR(pickup_datetime) BETWEEN 2020 AND 2030) AS is_plausible_date
    FROM base_staging
),

calculated_metrics AS (
    SELECT
        *,
        CASE
            WHEN trip_duration_min > 0
                THEN ROUND(trip_distance / (trip_duration_min / 60.0), 2)
            ELSE 0.0
        END AS speed_mph,
        CASE
            WHEN fare_amount > 0
                THEN ROUND((tip_amount / fare_amount) * 100.0, 2)
            ELSE 0.0
        END AS tip_percentage,
        ROUND(
            COALESCE(extra, 0)
            + COALESCE(mta_tax, 0)
            + COALESCE(improvement_surcharge, 0)
            + COALESCE(congestion_surcharge, 0)
            + COALESCE(airport_fee, 0)
            + COALESCE(cbd_congestion_fee, 0),
            2
        ) AS total_surcharges,
        (fare_amount < 0) AS is_adjustment,
        (trip_distance = 0) AS is_zero_distance
    FROM calculated_timestamps
),

evaluated_quality_rules AS (
    SELECT
        *,
        CASE
            WHEN trip_duration_min <= 0 THEN 'non_positive_duration'
            WHEN trip_duration_min > 180 THEN 'duration_over_180'
            WHEN speed_mph > 100 THEN 'implausible_speed'
            ELSE 'valid'
        END AS duration_issue
    FROM calculated_metrics
)

SELECT
    trip_key,
    vendor_id,
    ratecode_id,
    payment_type,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_date_id,
    dropoff_date_id,
    pickup_time_id,
    dropoff_time_id,
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
    total_surcharges,
    trip_duration_min,
    speed_mph,
    tip_percentage,
    duration_issue,
    is_adjustment,
    is_zero_distance,
    is_plausible_date,
    (duration_issue = 'valid') AS is_valid_duration,
    (
        duration_issue = 'valid'
        AND NOT is_adjustment
        AND NOT is_zero_distance
        AND fare_amount > 0
        AND total_amount > 0
        AND is_plausible_date
    ) AS is_valid_trip

FROM evaluated_quality_rules
