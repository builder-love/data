-- create a table that shows the latest projects and their watcher count
-- count of watchers by project

{{ config(
    materialized='table',
    unique_key='project_title || data_timestamp',
    tags=['latest_clean_data']
) }}

select 
  o.project_title,
  CAST(SUM(f.watcher_count) AS bigint) AS watcher_count,
  MAX(f.data_timestamp) AS data_timestamp

from {{ ref('latest_project_repos_watcher_count') }} f left join {{ ref('latest_project_repos') }} o
  on f.repo = o.repo

where o.project_title is not null 
and f.watcher_count is not null

group by 1
order by 2 desc