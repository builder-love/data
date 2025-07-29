-- normalized_contributor_dominant_language_top_1000.sql
-- create an incremental table in clean with data_timestamp intervals >20 days apart
-- project_repos_contributors raw table
-- 
{{ config(
    materialized='incremental',
    unique_key='dominant_language || data_timestamp',
    tags=['timestamp_normalized']
) }}

-- Call the macro with a limit of 1000
{{ generate_normalized_contributor_dominant_language(contributor_limit=1000) }}