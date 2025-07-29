-- models/api/top_projects_trend.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || report_date',
        tags=['api_data']
    ) 
}} 

{{ generate_top_projects_trend(
    top_projects_trend_model='normalized_top_projects_prod'
) }}