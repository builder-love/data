-- normalized_contributor_dominant_language.sql
-- create an incremental table in clean with data_timestamp intervals >20 days apart
-- project_repos_contributors raw table
-- 
{{ config(
    materialized='incremental',
    unique_key='dominant_language || data_timestamp',
    tags=['timestamp_normalized']
) }}

{% set initial_load_timestamp = '2025-05-22T09:11:30.833585Z' %}

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
        from {{ ref('latest_top_contributors') }}
        group by 1
    ),

    -- Identify the latest data_timestamp in the raw table
    load_timestamps AS (
        SELECT data_timestamp AS load_timestamps
        FROM raw_data_timestamps
        where data_timestamp >= '{{ max_clean_timestamp }}'::timestamp + INTERVAL '20 days'
    ),

    language_counts as (
        select 
        dominant_language,
        count(*) developer_count,
        max(data_timestamp) data_timestamp
        FROM {{ ref('latest_top_contributors') }} po
        WHERE po.data_timestamp in (select load_timestamps from load_timestamps)
        and dominant_language is not NULL
        group by 1
        order by 2 desc
    ),

    language_counts_with_row_count as (
        select *,
        count(*) over () as row_count
        from language_counts
    )

    select 
        dominant_language,
        developer_count,
        data_timestamp
    from language_counts_with_row_count
    where row_count <= ({{ mean_count }} * 1.5) and row_count >= ({{ mean_count }} * 0.5)


{% else %}

    SELECT
        dominant_language,
        count(*) developer_count,
        max(data_timestamp) data_timestamp
    FROM
        {{ ref('latest_top_contributors') }} po 
    WHERE po.data_timestamp = '{{ initial_load_timestamp }}'::timestamp
    and dominant_language is not NULL
    group by 1
    order by 2 desc
    
{% endif %}