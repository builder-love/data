-- models/clean/latest_project_organizations.sql
-- get the current metrics for each project organization

{{ config(
    materialized='table',
    unique_key='project_title || project_organization_url || data_timestamp',
    tags=['latest_clean_data']
) }}

select distinct 
  project_title,
  project_organization_url,
  NOW() AT TIME ZONE 'utc' as data_timestamp

from {{ ref('latest_project_repos') }}
where project_title is not null and project_organization_url is not null and project_organization_url <> 'https://github.com/'
