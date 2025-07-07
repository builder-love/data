-- models/api/top_50_projects_trend_no_forks.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || report_date',
        tags=['api_data']
    ) 
}} 

{{ generate_top_50_projects_trend(
    top_50_projects_trend_model='normalized_top_projects_no_forks_prod',
    top_50_projects_model='top_50_projects_no_forks'
) }}