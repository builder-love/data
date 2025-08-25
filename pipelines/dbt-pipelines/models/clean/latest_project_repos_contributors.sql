-- models/clean/latest_project_repos_contributors.sql
-- if clean.latest_project_repos_contributors table exists, replace it with the raw.latest_project_repos_contributors table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='contributor_unique_id_builder_love || repo || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT *
FROM {{ source('raw', 'latest_project_repos_contributors') }} 