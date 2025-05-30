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
  from {{ source('prod_schema', 'latest_top_projects_prod') }}
  order by weighted_score_sma desc 
  limit 50