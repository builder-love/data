-- create a table that shows the latest projects and their fork count
-- count of forks by project

{{ config(
    materialized='table',
    unique_key='project_title || data_timestamp',
    tags=['latest_clean_data']
) }}

select 
  o.project_title,
  SUM(f.fork_count) AS fork_count,
  MAX(f.data_timestamp) AS data_timestamp

from {{ ref('latest_project_repos_fork_count') }} f left join {{ ref('latest_project_repos') }} o
  on f.repo = o.repo

where o.project_title is not null 
and f.fork_count is not null

group by 1
order by 2 desc