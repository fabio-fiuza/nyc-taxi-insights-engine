{{ config(materialized='table') }}

WITH

payments_lookup AS (
    SELECT DISTINCT
        CAST(payment_type_id AS VARCHAR) AS payment_method_id,
        COALESCE(NULLIF(TRIM(payment_type), ''), 'Unknown') AS payment_method_type
    FROM {{ ref('payment_methods_lookup') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "payment_method_id"
    ]) }} AS payment_method_key,
    payment_method_id,
    payment_method_type

FROM payments_lookup