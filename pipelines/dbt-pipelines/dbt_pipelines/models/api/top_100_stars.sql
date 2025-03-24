-- models/api/top_100_stars.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title',
        tags=['api_data']
    ) 
}} 

select 
  o.project_title,
  TO_CHAR(MAX(f.data_timestamp), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp,
  sum(f.stargaze_count) stars

from {{ ref('latest_project_repos_stargaze_count') }} f left join {{ ref('latest_project_repos') }} o
  on f.repo = o.repo

where f.stargaze_count is not null

group by 1 
order by 3 desc
limit 100