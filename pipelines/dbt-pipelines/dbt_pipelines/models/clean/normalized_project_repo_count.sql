-- normalized_project_repo_count.sql
-- create an incremental table in clean with data_timestamp intervals >6 days apart
-- this dbt model references latest_active_distinct, and sources data from raw.crypto_ecosystems_raw_file

{{ config(
    materialized='incremental',
    unique_key='project_title || data_timestamp',
    tags=['timestamp_normalized']
) }}

{% set initial_load_timestamp = '2025-05-21T14:16:31.489559Z' %}

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

    WITH 

    raw_data_timestamps as (
        select data_timestamp, count(*) as record_count
        from {{ ref('latest_active_distinct_project_repos') }}
        group by 1
    ),

    -- Identify the latest data_timestamp in the latest_active_distinct_project_repos table
    load_timestamps AS (
        SELECT data_timestamp AS load_timestamps
        FROM raw_data_timestamps
        where data_timestamp >= '{{ max_clean_timestamp }}'::timestamp + INTERVAL '6 days'
    ),

    -- Select all records from the raw table
    active_repos AS (
    SELECT DISTINCT ON (repo)
        repo,
        data_timestamp
    FROM {{ ref('latest_active_distinct_project_repos') }}
    WHERE is_active = true
    and data_timestamp in (select load_timestamps from load_timestamps)
    ORDER BY repo, data_timestamp DESC -- IMPORTANT: DISTINCT ON requires ORDER BY to give predictable results
    )

    SELECT
    cer.project_title,
    COUNT(DISTINCT cer.repo) AS repo_count,
    MAX(ar.data_timestamp) AS data_timestamp
    FROM {{ source('raw', 'crypto_ecosystems_raw_file') }} AS cer INNER JOIN active_repos AS ar 
    ON cer.repo = ar.repo

    GROUP BY cer.project_title
    ORDER BY repo_count DESC

{% else %}

    -- Select all records from the raw table
    WITH active_repos AS (
    SELECT DISTINCT ON (repo)
        repo,
        data_timestamp
    FROM {{ ref('latest_active_distinct_project_repos') }}
    WHERE is_active = true
    and data_timestamp = '{{ initial_load_timestamp }}'::timestamp
    ORDER BY repo, data_timestamp DESC -- IMPORTANT: DISTINCT ON requires ORDER BY to give predictable results
    )

    SELECT
    cer.project_title,
    COUNT(DISTINCT cer.repo) AS repo_count,
    MAX(ar.data_timestamp) AS data_timestamp
    FROM {{ source('raw', 'crypto_ecosystems_raw_file') }} AS cer JOIN active_repos AS ar 
    ON cer.repo = ar.repo

    GROUP BY cer.project_title
    ORDER BY repo_count DESC
    
{% endif %}