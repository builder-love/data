-- models/api/top_50_projects.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || latest_data_timestamp',
        tags=['api_data']
    ) 
}} 

{{ generate_top_50_projects(
    top_50_projects_model='latest_top_projects_prod'
) }}