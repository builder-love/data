-- models/clean/latest_distinct_project_repos.sql
{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}

WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_project_repos') }}  -- Refers to the *normalized* table
)
SELECT DISTINCT
    repo,
    repo_source,
    data_timestamp
FROM {{ ref('normalized_project_repos') }}  -- Refers to the *normalized* table
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)