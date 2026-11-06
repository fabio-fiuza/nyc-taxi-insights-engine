-- dim_dates.sql
{{ config(materialized = 'table') }}

{%- set bounds = get_date_bounds('int_nyc_taxi__yellow_trips_enriched', 'pickup_datetime') -%}

{%- set min_date = bounds.min_date if bounds and bounds.min_date else '2025-01-01' -%}
{%- set max_date = bounds.max_date if bounds and bounds.max_date else '2027-12-31' -%}

WITH raw_dates AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('" ~ min_date ~ "' AS DATE)",
        end_date="CAST(DATEADD(day, 1, '" ~ max_date ~ "') AS DATE)"
    ) }}
),

enriched_dates AS (
    SELECT
        CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_id,
        CAST(date_day AS DATE) AS date,

        -- Years and Quarters attributes
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,

        -- Month attributes
        MONTH(date_day) AS month,
        MONTHNAME(date_day) AS month_name,

        -- Day attributes
        DAY(date_day) AS day,
        -- ISO weekday normalized to 0 = Monday and 6 = Sunday
        (DAYOFWEEKISO(date_day) - 1) AS weekday,
        CASE (DAYOFWEEKISO(date_day) - 1)
            WHEN 0 THEN 'Monday'
            WHEN 1 THEN 'Tuesday'
            WHEN 2 THEN 'Wednesday'
            WHEN 3 THEN 'Thursday'
            WHEN 4 THEN 'Friday'
            WHEN 5 THEN 'Saturday'
            WHEN 6 THEN 'Sunday'
        END AS weekday_name,

        -- Weekend attributes
        CASE
            WHEN DAYOFWEEKISO(date_day) IN (6, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM raw_dates
)

SELECT
    date_id,
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