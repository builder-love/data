-- models/clean/latest_distinct_project_repos.sql
{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}

WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('latest_project_repos') }} 
)
SELECT DISTINCT
    repo,
    repo_source,
    data_timestamp
FROM {{ ref('latest_project_repos') }} 
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)