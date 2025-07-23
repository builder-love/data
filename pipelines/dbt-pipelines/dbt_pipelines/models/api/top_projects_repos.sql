-- models/api/top_projects_repos.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || repo || latest_data_timestamp',
        tags=['api_data']
    ) 
}} 

select 
  project_title,
  TO_CHAR(first_seen_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS first_seen_timestamp,
  TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp,
  repo,
  fork_count,
  stargaze_count,
  watcher_count,
  weighted_score_index,
  repo_rank,
  quartile_bucket,
  repo_rank_category,
  predicted_is_dev_tooling,
  predicted_is_educational,
  predicted_is_scaffold,
  predicted_is_app,
  predicted_is_infrastructure

from {{ source('prod_schema', 'latest_top_project_repos_prod') }}