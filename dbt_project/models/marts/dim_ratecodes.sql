{{ config(materialized='table') }}

WITH

ratecodes_lookup AS (
    SELECT DISTINCT
        CAST(ratecode_id AS VARCHAR) AS ratecode_id,
        COALESCE(NULLIF(TRIM(ratecode_name), ''), 'Null/unknown') AS ratecode_name
    FROM {{ ref('ratecode_lookup') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "ratecode_id"
    ]) }} AS ratecode_key,
    ratecode_id,
    ratecode_name

FROM ratecodes_lookup