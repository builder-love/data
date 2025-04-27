-- create a table that shows the latest projects and their is_fork status
-- get ratio of forked/non-forked repos by project

{{ config(
    materialized='table',
    unique_key='project_title || data_timestamp',
    tags=['latest_clean_data']
) }}

select 
  o.project_title,
  count(*) as repo_count,
  SUM(f.is_fork::INTEGER) AS is_fork_count,
  COUNT(*) - SUM(f.is_fork::INTEGER) AS is_not_fork_count,
  SUM(f.is_fork::INTEGER)::NUMERIC / COUNT(*) AS is_fork_ratio,
  (1 - (SUM(f.is_fork::INTEGER)::NUMERIC / COUNT(*))) AS is_not_fork_ratio,
  MAX(f.data_timestamp) AS data_timestamp

from {{ ref('latest_project_repos_is_fork') }} f left join {{ ref('latest_project_repos') }} o
  on f.repo = o.repo

where o.project_title is not null 
and f.is_fork is not null

group by 1
order by 4