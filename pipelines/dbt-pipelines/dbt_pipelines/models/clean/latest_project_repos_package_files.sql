-- models/clean/latest_project_repos_package_files.sql
-- if clean.latest_project_repos_package_files table exists, replace it
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='repo || file_name || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT 
    repo,
    data_timestamp,
    package_manager,
    file_name,
    file_content

FROM {{ source('raw', 'latest_project_repos_package_files') }} 