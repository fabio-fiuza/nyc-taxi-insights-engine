-- dim_payments_methods.sql
{{ config(materialized='table') }}

WITH

payments_lookup AS (
    SELECT
        payment_type_id,
        payment_type
    FROM {{ ref('payment_methods_lookup') }}
)

SELECT

    {{ dbt_utils.generate_surrogate_key(['payment_type_id']) }}
        AS payment_method_sk,
    payment_type_id AS payment_method_id,
    payment_type AS payment_method_type

FROM payments_lookup
