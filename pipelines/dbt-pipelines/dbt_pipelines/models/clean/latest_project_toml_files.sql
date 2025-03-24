-- models/clean/latest_project_toml_files.sql
{{ config(
    materialized='table',
    unique_key='toml_file_data_url || data_timestamp',
    tags=['latest_clean_data']
) }}

WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_project_toml_files') }}  -- Refers to the *normalized* table
)
SELECT
    toml_file_data_url,
    data_timestamp
FROM {{ ref('normalized_project_toml_files') }}  -- Refers to the *normalized* table
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)