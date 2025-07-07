-- models/clean/latest_top_projects_no_forks.sql
-- calculation logic for weighted_score happens in clean/normalized_top_project.sql
-- calculates the simple moving average weighted score by project, and conveniently provides the list of top projects in latest report_date
-- this version drops forked repos

{{ config(
    materialized='table',
    unique_key='project_title || report_date',
    tags=['latest_clean_data']
) }}

{{ generate_latest_top_projects(
    top_projects_model='normalized_top_projects_no_forks'
) }}