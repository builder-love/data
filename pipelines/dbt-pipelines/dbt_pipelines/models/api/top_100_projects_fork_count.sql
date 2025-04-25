-- models/api/top_100_projects_fork_count.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title',
        tags=['api_data']
    ) 
}} 

select 
  project_title,
  TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp,
  fork_count

from {{ ref('latest_project_fork_count') }}

where project_title is not null

limit 100