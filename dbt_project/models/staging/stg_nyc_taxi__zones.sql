-- stg_nyc_taxi__zones.sql
{{ config(materialized='view') }}

WITH

source_seed AS (
    SELECT * FROM {{ ref('taxi_zone_lookup') }}
),

renamed AS (

    SELECT

    -- Identifiers
        LocationID AS location_id,
        Borough AS borough,
        Zone AS zone,
        service_zone AS service_zone

    FROM source_seed

)

SELECT * FROM renamed
