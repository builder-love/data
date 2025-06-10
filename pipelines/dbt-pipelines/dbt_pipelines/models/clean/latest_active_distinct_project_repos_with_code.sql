-- models/clean/latest_active_distinct_project_repos_with_code.sql
-- if clean.latest_active_distinct_project_repos_with_code table exists, replace it
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT 
    repo,
    data_timestamp,
    repo_source

FROM {{ ref('latest_active_distinct_project_repos') }} 
where (is_active = true and is_archived <> true)
and repo not in (select repo from {{ ref('latest_project_repos_no_code') }})