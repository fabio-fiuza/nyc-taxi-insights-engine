{{
    config(
        materialized = 'incremental',
        unique_key = 'trip_key',
        cluster_by = ['pickup_date_id', 'pickup_location_id']
    )
}}

WITH int_trips AS (
    SELECT * FROM {{ ref('int_nyc_taxi__yellow_trips_enriched') }}
),

fct_trips AS (
    SELECT
        t.trip_key,

        COALESCE(
            v.vendor_key,
            {{ dbt_utils.generate_surrogate_key(["CAST('99' AS VARCHAR)"]) }}
        ) AS vendor_key,
        COALESCE(d_pickup.date_id, t.pickup_date_id) AS pickup_date_id,
        COALESCE(d_dropoff.date_id, t.dropoff_date_id) AS dropoff_date_id,
        COALESCE(tm_pickup.time_id, t.pickup_time_id) AS pickup_time_id,
        COALESCE(tm_dropoff.time_id, t.dropoff_time_id) AS dropoff_time_id,
        COALESCE(
            p.payment_method_key,
            {{ dbt_utils.generate_surrogate_key(["CAST('5' AS VARCHAR)"]) }}
        ) AS payment_method_key,
        COALESCE(
            r.ratecode_key,
            {{ dbt_utils.generate_surrogate_key(["CAST('99' AS VARCHAR)"]) }}
        ) AS ratecode_key,
        COALESCE(loc_pickup.location_id, t.pickup_location_id)
            AS pickup_location_id,
        COALESCE(loc_dropoff.location_id, t.dropoff_location_id)
            AS dropoff_location_id,

        t.pickup_datetime AS tpep_pickup_datetime,
        t.dropoff_datetime AS tpep_dropoff_datetime,
        t.store_and_fwd_flag,
        t.passenger_count,
        t.trip_distance,
        t.trip_duration_min,
        t.speed_mph,
        t.tip_percentage,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.airport_fee,
        t.cbd_congestion_fee,
        t.total_amount,
        t.total_surcharges,
        t.duration_issue,
        t.is_adjustment,
        t.is_zero_distance,
        t.is_plausible_date,
        t.is_valid_duration,
        t.is_valid_trip

    FROM int_trips AS t
    LEFT JOIN {{ ref('dim_vendors') }} AS v
        ON t.vendor_id = v.vendor_id
    LEFT JOIN {{ ref('dim_dates') }} AS d_pickup
        ON t.pickup_date_id = d_pickup.date_id
    LEFT JOIN {{ ref('dim_dates') }} AS d_dropoff
        ON t.dropoff_date_id = d_dropoff.date_id
    LEFT JOIN {{ ref('dim_times') }} AS tm_pickup
        ON t.pickup_time_id = tm_pickup.time_id
    LEFT JOIN {{ ref('dim_times') }} AS tm_dropoff
        ON t.dropoff_time_id = tm_dropoff.time_id
    LEFT JOIN {{ ref('dim_payments_methods') }} AS p
        ON t.payment_type = p.payment_method_id
    LEFT JOIN {{ ref('dim_ratecodes') }} AS r
        ON t.ratecode_id = r.ratecode_id
    LEFT JOIN {{ ref('dim_zones') }} AS loc_pickup
        ON t.pickup_location_id = loc_pickup.location_id
    LEFT JOIN {{ ref('dim_zones') }} AS loc_dropoff
        ON t.dropoff_location_id = loc_dropoff.location_id

    {% if is_incremental() %}
        WHERE
            t.pickup_datetime
            > (SELECT MAX(tpep_pickup_datetime) FROM {{ this }})
    {% endif %}
)

SELECT * FROM fct_trips
