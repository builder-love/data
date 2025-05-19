-- models/clean/latest_distinct_projects.sql
{{ config(
    materialized='table',
    unique_key='project_title || data_timestamp',
    tags=['latest_clean_data']
) }}


SELECT DISTINCT
    project_title,
    data_timestamp
FROM {{ ref('latest_project_repos') }} 