-- models/api/top_50_projects_contributor_trend.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || latest_data_timestamp',
        tags=['api_data']
    ) 
}} 

with top_50 as (
  select project_title 
  from {{ source('prod', 'latest_top_projects') }}
  order by weighted_score desc 
  limit 50 
)

select 
  project_title,
  contributor_count,
  TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp

from {{ source('prod', 'normalized_project_contributor_count') }}

where project_title in (select project_title from top_50)

order by project_title, data_timestamp desc