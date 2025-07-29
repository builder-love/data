-- models/clean/four_week_change_project_contributor_count.sql
-- the periodicity of the data is monthly so we look back one period to get the 4 week change

{{ 
    config(
        materialized='view',
        unique_key='project_title || data_timestamp',
        tags=['period_change_data']
    ) 
}} 

{{ generate_four_week_change_project_contributor_count(
    contributor_count_model='normalized_project_contributor_count'
) }}