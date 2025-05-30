-- latest_project_repos_commit_count.sql
-- create a view that shows the latest project repos and their commit count

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}


WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_project_repos_commit_count') }}  
)
SELECT
    repo,
    commit_count,
    data_timestamp
FROM {{ ref('normalized_project_repos_commit_count') }} 
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)