-- models/clean/latest_project_organizations.sql
{{ config(
    materialized='table',
    unique_key='project_title || project_organization_url || data_timestamp',
    tags=['latest_clean_data']
) }}

WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_project_organizations') }}  -- Refers to the *normalized* table
)
SELECT
    project_title,
    project_organization_url,
    data_timestamp
FROM {{ ref('normalized_project_organizations') }}  -- Refers to the *normalized* table
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)