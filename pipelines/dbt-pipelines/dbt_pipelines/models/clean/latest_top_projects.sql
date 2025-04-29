-- models/clean/latest_top_projects.sql
-- calculation logic for weighted_score happens in clean/normalized_top_project.sql
-- calculates the simple moving average weighted score by project, and conveniently provides the list of top projects in latest report_date

{{ config(
    materialized='table',
    unique_key='project_title || report_date',
    tags=['latest_clean_data']
) }}

WITH project_sma AS (
  SELECT
    ntp.*,
    -- Calculate the 4-week simple moving average (current week + 3 preceding weeks)
    AVG(weighted_score) OVER (
        PARTITION BY project_title 
        ORDER BY report_date ASC   
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW -- Window: current row + 3 previous rows
    ) AS weighted_score_4wk_sma
  FROM
    {{ ref('normalized_top_projects') }} ntp
)

select 
  project_title,
  report_date,
  data_timestamp,
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
  weighted_score_4wk_sma,
  project_rank,
  quartile_bucket,
  project_rank_category

from project_sma

where report_date = (select max(report_date) report_date from project_sma)