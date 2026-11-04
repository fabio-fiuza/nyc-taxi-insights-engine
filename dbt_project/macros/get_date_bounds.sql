-- get_date_bounds.sql
{% macro get_date_bounds(model_name, date_column) %}
    {%- set fallback_min_date = '2025-01-01' -%}
    {%- set fallback_max_date = '2027-12-31' -%}

    {%- set query -%}
        SELECT 
            MIN(CAST({{ date_column }} AS DATE)) AS min_date,
            MAX(CAST({{ date_column }} AS DATE)) AS max_date
        FROM {{ ref(model_name) }}
    {%- endset -%}

    {%- set results = run_query(query) -%}

    {%- if execute -%}
        {%- set min_date = results.columns[0].values()[0] -%}
        {%- set max_date = results.columns[1].values()[0] -%}
    {%- else -%}
        {%- set min_date = fallback_min_date -%}
        {%- set max_date = fallback_max_date -%}
    {%- endif -%}

    {{ return({'min_date': min_date, 'max_date': max_date}) }}
{% endmacro %}