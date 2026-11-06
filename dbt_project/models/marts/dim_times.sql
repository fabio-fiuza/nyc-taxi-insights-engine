{{ config(materialized='table') }}

WITH time_lookup AS (
    SELECT
        hour,
        period
    FROM {{ ref('time_lookup') }}
)

SELECT DISTINCT
    CAST(hour AS INTEGER) AS time_id,
    TRIM(period) AS period
FROM time_lookup
