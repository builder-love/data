-- models/api/top_projects_trend.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || report_date',
        tags=['api_data']
    ) 
}} 

select 
  project_title,
  TO_CHAR(report_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS report_date,
  TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp,
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
  RANK() OVER (
    ORDER BY (normalized_fork_count) DESC
  ) AS fork_count_rank,
  RANK() OVER (
    ORDER BY (normalized_stargaze_count) DESC
  ) AS stargaze_count_rank,
  RANK() OVER (
    ORDER BY (normalized_commit_count) DESC
  ) AS commit_count_rank,
  RANK() OVER (
    ORDER BY (normalized_contributor_count) DESC
  ) AS contributor_count_rank,
  RANK() OVER (
    ORDER BY (normalized_watcher_count) DESC
  ) AS watcher_count_rank,
  RANK() OVER (
    ORDER BY (normalized_is_not_fork_ratio) DESC
  ) AS is_not_fork_ratio_rank,
  RANK() OVER (
    ORDER BY (normalized_commit_count_pct_change_over_4_weeks) DESC
  ) AS commit_count_pct_change_over_4_weeks_rank,
  RANK() OVER (
    ORDER BY (normalized_contributor_count_pct_change_over_4_weeks) DESC
  ) AS contributor_count_pct_change_over_4_weeks_rank,
  RANK() OVER (
    ORDER BY (normalized_fork_count_pct_change_over_4_weeks) DESC
  ) AS fork_count_pct_change_over_4_weeks_rank,
  RANK() OVER (
    ORDER BY (normalized_stargaze_count_pct_change_over_4_weeks) DESC
  ) AS stargaze_count_pct_change_over_4_weeks_rank,
  RANK() OVER (
    ORDER BY (normalized_watcher_count_pct_change_over_4_weeks) DESC
  ) AS watcher_count_pct_change_over_4_weeks_rank,
  RANK() OVER (
    ORDER BY (normalized_is_not_fork_ratio_pct_change_over_4_weeks) DESC
  ) AS is_not_fork_ratio_pct_change_over_4_weeks_rank,
  RANK() OVER (
    PARTITION BY quartile_bucket 
    ORDER BY project_rank DESC
  ) AS quartile_project_rank,
  project_rank as overall_project_rank

from {{ source('prod_schema', 'normalized_top_projects_prod') }}

where project_title is not null and report_date is not null and data_timestamp is not null