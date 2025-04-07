-- create a view that shows the latest project repos and their language makeup

{{ config(
    materialized='table',
    unique_key='repo || language_name || data_timestamp',
    tags=['latest_clean_data']
) }}

WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_project_repos_languages') }}  
)
SELECT
    repo,
    language_name,
    size,
    repo_languages_total_bytes,
    data_timestamp
FROM {{ ref('normalized_project_repos_languages') }}  
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)