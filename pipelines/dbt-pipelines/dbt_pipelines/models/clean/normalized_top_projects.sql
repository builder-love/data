-- normalized_top_projects.sql
-- create an incremental table in clean with data_timestamp intervals >6 days apart
-- latest_top_projects raw table
-- 
{{ config(
    materialized='incremental',
    unique_key='project_title || data_timestamp',
    tags=['timestamp_normalized']
) }}

{% set initial_load_timestamp = '2025-03-31T13:55:41.132697Z' %}

{% if is_incremental() %}

    {% set max_clean_timestamp_query %}
        SELECT MAX(data_timestamp) FROM {{ this }}
    {% endset %}

    {% set max_clean_timestamp_results = run_query(max_clean_timestamp_query) %}

    {% if execute %}
        {% set max_clean_timestamp = max_clean_timestamp_results.columns[0].values()[0] %}
        {% if max_clean_timestamp == none %}
            {% set max_clean_timestamp = (modules.datetime.datetime.fromisoformat(initial_load_timestamp.replace('Z', '+00:00'))).isoformat() %}
        {% endif %} 
    {% else %}
        {% set max_clean_timestamp = (modules.datetime.datetime.fromisoformat(initial_load_timestamp.replace('Z', '+00:00'))).isoformat() %}
    {% endif %}

    -- get stats for outlier exclusion
    {% set stats_query %}
        -- Calculate the count of records for each data_timestamp in the clean table
        with record_counts AS (
            SELECT 
                data_timestamp, 
                COUNT(*) AS record_count
            FROM {{ this }} -- Use {{ this }} to refer to the current model (clean table)
            GROUP BY data_timestamp
        )

        -- Calculate the mean and standard deviation of the record counts from the clean table
        SELECT 
            AVG(record_count) AS mean_count, 
            coalesce(STDDEV(record_count), 0) AS stddev_count
        FROM record_counts
    {% endset %}

    {% set stats_results = run_query(stats_query) %}

    {% if execute %}
        {% set mean_count = stats_results.columns[0].values()[0] %}
        {% set stddev_count = stats_results.columns[1].values()[0] %}
    {% else %}
        {% set mean_count = 0 %}
        {% set stddev_count = 0 %}
    {% endif %}
    

    WITH 

    raw_data_timestamps as (
        select data_timestamp, count(*) as record_count
        from {{ source('clean', 'latest_top_projects') }}
        group by 1
    ),

    -- Identify the latest data_timestamp in the raw table
    load_timestamps AS (
        SELECT data_timestamp AS load_timestamps
        FROM raw_data_timestamps
        where data_timestamp >= '{{ max_clean_timestamp }}'::timestamp + INTERVAL '6 days'
        AND record_count <= ({{ mean_count }} * 1.5)
        AND record_count >= ({{ mean_count }} * 0.5)
    )

    -- Select all records from the raw table
    SELECT
        po.*
    FROM
        {{ source('clean', 'latest_top_projects') }} po

    WHERE po.data_timestamp in (select load_timestamps from load_timestamps)

{% else %}

    -- Select all records from the raw table
    SELECT
        po.*
    FROM
        {{ source('clean', 'latest_top_projects') }} po 
    WHERE po.data_timestamp = '{{ initial_load_timestamp }}'::timestamp
    
{% endif %}