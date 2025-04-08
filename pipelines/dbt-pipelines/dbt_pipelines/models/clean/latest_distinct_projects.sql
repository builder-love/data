-- models/clean/latest_distinct_projects.sql
{{ config(
    materialized='table',
    unique_key='project_title || data_timestamp',
    tags=['latest_clean_data']
) }}

WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_project_organizations') }} 
)
SELECT DISTINCT
    project_title,
    data_timestamp
FROM {{ ref('normalized_project_organizations') }} 
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)