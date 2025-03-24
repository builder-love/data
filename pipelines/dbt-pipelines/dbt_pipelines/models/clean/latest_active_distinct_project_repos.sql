-- models/clean/latest_active_distinct_project_repos.sql
-- if clean.latest_active_distinct_project_repos table exists, replace it with the raw.latest_active_distinct_project_repos table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT *
FROM {{ source('raw', 'latest_active_distinct_project_repos') }} 