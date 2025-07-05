-- models/api/project_outliers.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title',
        tags=['api_data']
    ) 
}} 

select 
  project_title,
  TO_CHAR(report_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS report_date,
  repo_count_pct_change_over_4_weeks,
  repo_count as current_repo_count,
  (coalesce(repo_count,0)/nullif((1+repo_count_pct_change_over_4_weeks),0))::BIGINT  as previous_repo_count,
  commit_count_pct_change_over_4_weeks,
  commit_count as current_commit_count,
  (coalesce(commit_count,0)/nullif((1+commit_count_pct_change_over_4_weeks),0))::BIGINT  as previous_commit_count,
  contributor_count_pct_change_over_4_weeks,
  contributor_count as current_contributor_count,
  (coalesce(contributor_count,0)/nullif((1+contributor_count_pct_change_over_4_weeks),0))::BIGINT  as previous_contributor_count,
  fork_count_pct_change_over_4_weeks,
  fork_count as current_fork_count,
  (coalesce(fork_count,0)/nullif((1+fork_count_pct_change_over_4_weeks),0))::BIGINT  as previous_fork_count,
  stargaze_count_pct_change_over_4_weeks,
  stargaze_count as current_stargaze_count,
  (coalesce(stargaze_count,0)/nullif((1+stargaze_count_pct_change_over_4_weeks),0))::BIGINT  as previous_stargaze_count,
  watcher_count_pct_change_over_4_weeks,
  watcher_count as current_watcher_count,
  (coalesce(watcher_count,0)/nullif((1+watcher_count_pct_change_over_4_weeks),0))::BIGINT  as previous_watcher_count,
  is_not_fork_ratio_pct_change_over_4_weeks,
  is_not_fork_ratio as current_is_not_fork_ratio,
  (coalesce(is_not_fork_ratio,0)/nullif((1+is_not_fork_ratio_pct_change_over_4_weeks),0))::NUMERIC  as previous_is_not_fork_ratio,
  absolute_project_rank_change_over_4_weeks,
  rank_of_project_rank_change_over_4_weeks

from {{ source('prod_schema', 'latest_top_projects_prod') }}