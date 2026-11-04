-- dim_dates.sql
{{ config(materialized = 'table') }}

{%- set bounds = get_date_bounds('int_nyc_taxi__yellow_trips_enriched', 'pickup_datetime') -%}

{%- set min_date = bounds.min_date if bounds and bounds.min_date else '2020-01-01' -%}
{%- set max_date = bounds.max_date if bounds and bounds.max_date else '2026-12-31' -%}

WITH raw_dates AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('" ~ min_date ~ "' AS DATE)",
        end_date="CAST(DATEADD(day, 1, '" ~ max_date ~ "') AS DATE)"
    ) }}
),

enriched_dates AS (
    SELECT
        -- Key and Date attributes
        CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_sk,
        CAST(date_day AS DATE) AS date,

        -- Years and Quarters attributes
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,

        -- Month attributes
        MONTH(date_day) AS month,
        MONTHNAME(date_day) AS month_name,

        -- Day attributes
        DAY(date_day) AS day,
        (DAYOFWEEKISO(date_day) - 1) AS weekday,
        DAYNAME(date_day) AS weekday_name,

        -- Weekend attributes
        CASE
            WHEN DAYOFWEEKISO(date_day) IN (6, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM raw_dates
)

SELECT
    date_sk,
    date,
    year,
    quarter,
    month,
    month_name,
    day,
    weekday,
    weekday_name,
    is_weekend
FROM enriched_dates
ORDER BY date ASC
