-- dim_ratecodes.sql
{{ config(materialized='table') }}

WITH

ratecodes_lookup AS (
    SELECT 
        ratecode_id,
        ratecode_name
    FROM {{ ref('ratecode_lookup') }}
)

SELECT

    {{ dbt_utils.generate_surrogate_key(['ratecode_id']) }} AS ratecode_sk,
    ratecode_id,
    ratecode_name

FROM ratecodes_lookup