-- models/api/top_50_projects.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || latest_data_timestamp',
        tags=['api_data']
    ) 
}} 

  select 
    project_title,
    weighted_score,
    TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp
  from {{ ref('latest_top_projects') }}
  order by weighted_score desc 
  limit 50