-- create a table that shows the latest project repos and their is_fork status

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}


WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_project_repos_is_fork') }} 
)
SELECT
    repo,
    is_fork,
    data_timestamp
FROM {{ ref('normalized_project_repos_is_fork') }} 
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)