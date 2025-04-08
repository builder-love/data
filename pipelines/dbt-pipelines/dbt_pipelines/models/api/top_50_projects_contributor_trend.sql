-- models/api/top_50_projects_contributor_trend.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || latest_data_timestamp',
        tags=['api_data']
    ) 
}} 

select 
  project_title,
  contributor_count,
  TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp

from {{ ref('normalized_project_contributor_count') }}

where project_title in (select project_title from {{ ref('top_50_projects') }})

order by project_title, data_timestamp desc