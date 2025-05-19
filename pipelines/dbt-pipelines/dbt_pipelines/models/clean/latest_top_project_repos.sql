-- models/clean/latest_top_project_repos.sql
-- convenience table for joining top_repos with project_title
{{ config(
    materialized='table',
    unique_key='project_title || repo || report_date',
    tags=['latest_clean_data']
) }}

select 
  lpr.project_title,
  TO_CHAR(ltr.data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp,
  ltr.repo,
  ltr.fork_count,
  ltr.stargaze_count,
  ltr.watcher_count,
  ltr.weighted_score_index,
  ltr.repo_rank,
  ltr.quartile_bucket,
  ltr.repo_rank_category

from {{ ref('latest_top_repos') }} ltr left join {{ ref('latest_project_repos') }} lpr
  on ltr.repo = lpr.repo

order by ltr.weighted_score_index desc