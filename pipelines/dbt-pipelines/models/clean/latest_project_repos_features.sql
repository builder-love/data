-- models/clean/latest_project_repos_features.sql
-- if clean.latest_project_repos_features table exists, replace it with the raw.latest_project_repos_features table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT *
FROM {{ source('raw', 'latest_project_repos_features') }} 