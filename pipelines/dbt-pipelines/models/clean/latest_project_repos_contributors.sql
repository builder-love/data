-- models/clean/latest_project_repos_contributors.sql
-- if clean.latest_project_repos_contributors table exists, replace it with the raw.latest_project_repos_contributors table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='contributor_unique_id_builder_love || repo || data_timestamp',
    tags=['latest_clean_data']
) }}

-- the raw schema table has multiple data_timestamp values since we are only processing new data in raw.project_repos_contributors
-- for the clean schema table, we can use today's date as the data_timestamp
SELECT 
    contributor_unique_id_builder_love,
    repo,
    contributor_contributions,
    {{ dbt.current_timestamp() }}::timestamp as data_timestamp
FROM {{ source('raw', 'latest_project_repos_contributors') }} 