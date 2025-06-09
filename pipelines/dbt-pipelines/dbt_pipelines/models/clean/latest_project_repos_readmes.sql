-- models/clean/latest_project_repos_readmes.sql
-- if clean.latest_project_repos_readmes table exists, replace it with the raw.latest_project_repos_readmes table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT *
FROM {{ source('raw', 'latest_project_repos_readmes') }} 