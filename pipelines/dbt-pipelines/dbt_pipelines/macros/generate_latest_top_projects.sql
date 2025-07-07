-- macros/generate_latest_top_projects.sql

{% macro generate_latest_top_projects(
    top_projects_model
) %}

select 
  project_title,
  report_date,
  data_timestamp,
  repo_count,
  fork_count,
  stargaze_count,
  commit_count,
  contributor_count,
  watcher_count,
  is_not_fork_ratio,
  commit_count_pct_change_over_4_weeks,
  contributor_count_pct_change_over_4_weeks,
  fork_count_pct_change_over_4_weeks,
  stargaze_count_pct_change_over_4_weeks,
  watcher_count_pct_change_over_4_weeks,
  is_not_fork_ratio_pct_change_over_4_weeks,
  repo_count_pct_change_over_4_weeks,
  normalized_fork_count,
  normalized_stargaze_count,
  normalized_commit_count,
  normalized_contributor_count,
  normalized_watcher_count,
  normalized_is_not_fork_ratio,
  normalized_commit_count_pct_change_over_4_weeks,
  normalized_contributor_count_pct_change_over_4_weeks,
  normalized_fork_count_pct_change_over_4_weeks,
  normalized_stargaze_count_pct_change_over_4_weeks,
  normalized_watcher_count_pct_change_over_4_weeks,
  normalized_is_not_fork_ratio_pct_change_over_4_weeks,
  weighted_score,
  weighted_score_index,
  weighted_score_sma,
  prior_4_weeks_weighted_score,
  project_rank,
  prior_4_weeks_project_rank,
  absolute_project_rank_change_over_4_weeks,
  rank_of_project_rank_change_over_4_weeks,
  quartile_bucket,
  project_rank_category

from {{ ref(top_projects_model) }} 

where report_date = (select max(report_date) report_date from {{ ref(top_projects_model) }})

{% endmacro %}

