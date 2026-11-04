{{ config(materialized='table') }}

WITH time_lookup AS (
    SELECT
        hour,
        period
    FROM {{ ref('time_lookup') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['hour']) }} AS time_sk,
    hour AS hour_of_day,
    period
FROM time_lookup
ORDER BY hour ASC
