-- models/api/top_projects.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || latest_data_timestamp',
        tags=['api_data']
    ) 
}} 

{{ generate_top_projects_view(
    top_projects_view_model='latest_top_projects_prod'
) }}