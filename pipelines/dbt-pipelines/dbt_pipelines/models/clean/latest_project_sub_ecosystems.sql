-- models/clean/latest_project_sub_ecosystems.sql
{{ config(
    materialized='table',
    unique_key='project_title || sub_ecosystem || data_timestamp',
    tags=['latest_clean_data']
) }}

WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_project_sub_ecosystems') }}  -- Refers to the *normalized* table
)
SELECT
    project_title,
    sub_ecosystem,
    data_timestamp
FROM {{ ref('normalized_project_sub_ecosystems') }}  -- Refers to the *normalized* table
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)