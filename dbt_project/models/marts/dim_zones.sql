{{ config(materialized='table') }}

WITH

stg_zones AS (
    SELECT
        location_id,
        borough,
        zone_name,
        service_zone
    FROM {{ ref('stg_nyc_taxi__zones') }}
)

SELECT
    location_id,
    borough,
    zone_name,
    service_zone

FROM stg_zones