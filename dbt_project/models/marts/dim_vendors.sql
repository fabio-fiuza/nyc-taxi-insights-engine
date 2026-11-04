-- dim_vendors.sql
{{ config(materialized='table')}}

WITH

vendors_lookup AS (
    SELECT * FROM {{ ref('vendors_lookup') }}
)

SELECT

    {{ dbt_utils.generate_surrogate_key(['vendor_id']) }} AS vendor_sk,
    vendor_id,
    vendor_name

FROM vendors_lookup
