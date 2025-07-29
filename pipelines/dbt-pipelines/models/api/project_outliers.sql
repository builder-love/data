-- models/api/project_outliers.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title',
        tags=['api_data']
    ) 
}} 

{{ generate_project_outliers(
    project_outliers_model='latest_top_projects_prod'
) }}