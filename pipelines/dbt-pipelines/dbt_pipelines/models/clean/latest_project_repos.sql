-- models/clean/latest_project_repos.sql
{{ config(
    materialized='table',
    unique_key='project_title || repo || data_timestamp',
    tags=['latest_clean_data']
) }}

with projects as (
select 
    project_title,
    repo,
    SPLIT_PART( -- Second split: Split 'github.com' by '.' and take the 1st part
        SPLIT_PART(repo, '/', 3), -- First split: Split by '/' and take the 3rd part ('github.com')
        '.',
        1
    ) AS repo_source,
    'https://github.com/' || split_part(repo, '/', 4) AS project_organization_url,
    sub_ecosystems

from {{ source('raw', 'crypto_ecosystems_raw_file') }}

),

distinct_project_repo as (
select distinct on (project_title,repo,project_organization_url)
    project_title,
    repo,
    project_organization_url,
    repo_source,
    sub_ecosystems,
    NOW() AT TIME ZONE 'utc' as data_timestamp

from projects
)

select * from distinct_project_repo