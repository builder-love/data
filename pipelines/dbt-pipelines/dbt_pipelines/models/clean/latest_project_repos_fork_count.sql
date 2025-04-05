-- create a table that shows the latest project repos and their fork count

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}


WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_project_repos_fork_count') }}  -- Refers to the *normalized* table
)
SELECT
    repo,
    fork_count,
    data_timestamp
FROM {{ ref('normalized_project_repos_fork_count') }}  -- Refers to the *normalized* table
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)