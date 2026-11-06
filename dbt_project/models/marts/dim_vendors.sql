{{ config(materialized='table') }}

WITH

vendors_lookup AS (
    SELECT DISTINCT
        COALESCE(CAST(vendor_id AS VARCHAR), '99') AS vendor_id,
        COALESCE(NULLIF(TRIM(vendor_name), ''), 'Null/unknown') AS vendor_name
    FROM {{ ref('vendors_lookup') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "vendor_id"
    ]) }} AS vendor_key,

    vendor_id,
    vendor_name

FROM vendors_lookup