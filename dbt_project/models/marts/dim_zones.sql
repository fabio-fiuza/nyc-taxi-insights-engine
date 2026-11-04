-- dim_zones.sql
{{ config(materialized='table') }}

WITH

stg_zones AS (
    SELECT * FROM {{ ref('stg_nyc_taxi__zones') }}
)

SELECT

    {{ dbt_utils.generate_surrogate_key(['location_id']) }} AS zone_sk,
    location_id AS zone_id,
    borough,
    zone AS zone_name,
    service_zone

FROM stg_zones
