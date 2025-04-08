-- create a table that shows the latest projects and their stargaze count
-- count of stargazers by project

{{ config(
    materialized='table',
    unique_key='project_title || data_timestamp',
    tags=['latest_clean_data']
) }}

select 
  o.project_title,
  SUM(f.stargaze_count) AS stargaze_count,
  MAX(f.data_timestamp) AS data_timestamp

from {{ ref('latest_project_repos_stargaze_count') }} f left join {{ ref('latest_project_repos') }} o
  on f.repo = o.repo

where f.stargaze_count is not null

group by 1
order by 2 desc