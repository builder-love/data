-- models/clean/four_week_change_project_commit_count_no_forks.sql
-- this table updates weekly, so we take a shortcut and lag 3 records to get the 4 week change
-- in this version we drop forked repos

{{ 
    config(
        materialized='view',
        unique_key='project_title || data_timestamp',
        tags=['period_change_data']
    ) 
}} 

{{ generate_four_week_change_project_commit_count(
    commit_count_model='normalized_project_commit_count_no_forks'
) }}