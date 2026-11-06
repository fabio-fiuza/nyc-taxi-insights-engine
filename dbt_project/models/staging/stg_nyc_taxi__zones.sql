-- stg_nyc_taxi__zones.sql
{{ config(materialized='view') }}

WITH

source_seed AS (
    SELECT *
    FROM {{ ref('taxi_zone_lookup') }}
),

renamed AS (
    SELECT
        CAST(LocationID AS VARCHAR) AS location_id,
        TRIM(Borough) AS borough,
        TRIM(Zone) AS zone_name,
        TRIM(service_zone) AS service_zone
    FROM source_seed
)

SELECT * FROM renamed
