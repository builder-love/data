-- normalized_project_toml_files.sql
-- create an incremental table in clean with data_timestamp intervals >25 days apart
-- project_toml_files raw table
-- 
{{ config(
    materialized='incremental',
    unique_key='toml_file_data_url || data_timestamp',
    tags=['timestamp_normalized']
) }}

{% set initial_load_timestamp = '2025-02-13T06:45:09.319014Z' %}  -- Define the variable

{% if is_incremental() %}
    -- Get the latest timestamp from the *target* (clean) table
    {% set max_clean_timestamp_query %}
        SELECT MAX(data_timestamp) FROM {{ this }}
    {% endset %}

    {% set max_clean_timestamp_results = run_query(max_clean_timestamp_query) %}

    {% if execute %}
        {# Return the first column #}
        {% set max_clean_timestamp = max_clean_timestamp_results.columns[0].values()[0] %}
    {% else %}
        {% set max_clean_timestamp = none %}
    {% endif %}

{% endif %}

WITH
  raw_data AS (
    SELECT
        *,
        (select max(data_timestamp) from {{ source('raw', 'project_toml_files') }}) as max_raw_timestamp
    FROM
      {{ source('raw', 'project_toml_files') }}
  )
SELECT
    *
FROM
  raw_data
{% if is_incremental() %}
  WHERE max_raw_timestamp >= '{{max_clean_timestamp}}'::timestamp + INTERVAL '25 days'
{% else %}
  WHERE data_timestamp = '{{ initial_load_timestamp }}'::timestamp  -- Filter by timestamp on initial load
{% endif %}