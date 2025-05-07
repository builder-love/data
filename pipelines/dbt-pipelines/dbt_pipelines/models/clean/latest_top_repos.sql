-- models/clean/latest_top_repos.sql
-- calculate top_repo score
-- metrics and weights include: 
-- because forked repos inherit the parent repo's commits and contributors we avoid those metrics when scoring a repo
-- calculate weighted score using the following metrics and weights:
-- total all-time fork count (.20),
-- total all-time stargaze count (.20),  
-- total all-time watcher count (.10),
-- one month change fork count (.20), 
-- one month change stargaze count (.20), 
-- one month change watcher count (.10)

{{ config(
    materialized='table',
    unique_key='repo || report_date',
    tags=['latest_clean_data']
) }}

WITH latest_four_week_change_repos_fork_count as (
    select repo, data_timestamp, fork_count_pct_change_over_4_weeks
    from {{ ref('four_week_change_project_repos_fork_count') }}
    where data_timestamp = (select max(data_timestamp) from {{ ref('four_week_change_project_repos_fork_count') }})
),

latest_four_week_change_repos_stargaze_count as (
    select repo, data_timestamp, stargaze_count_pct_change_over_4_weeks
    from {{ ref('four_week_change_project_repos_stargaze_count') }}
    where data_timestamp = (select max(data_timestamp) from {{ ref('four_week_change_project_repos_stargaze_count') }})
),

latest_four_week_change_repos_watcher_count as (
    select repo, data_timestamp, watcher_count_pct_change_over_4_weeks
    from {{ ref('four_week_change_project_repos_watcher_count') }}
    where data_timestamp = (select max(data_timestamp) from {{ ref('four_week_change_project_repos_watcher_count') }})
),

all_metrics as (
  select 
    p.repo,
    f.data_timestamp as fork_data_timestamp, 
    s.data_timestamp as stargaze_data_timestamp, 
    w.data_timestamp as watcher_data_timestamp, 
    fork_change.data_timestamp as fork_change_data_timestamp, 
    stargaze_change.data_timestamp as stargaze_change_data_timestamp,
    watcher_change.data_timestamp as watcher_change_data_timestamp,
    GREATEST(f.data_timestamp, s.data_timestamp, w.data_timestamp, fork_change.data_timestamp, stargaze_change.data_timestamp, watcher_change.data_timestamp) row_data_timestamp,
    f.fork_count,
    s.stargaze_count,
    w.watcher_count,
    fork_change.fork_count_pct_change_over_4_weeks,
    stargaze_change.stargaze_count_pct_change_over_4_weeks,
    watcher_change.watcher_count_pct_change_over_4_weeks
    
  from {{ source('clean', 'latest_active_distinct_project_repos') }} p left join {{ ref('latest_project_repos_fork_count') }} f 
    on p.repo = f.repo left join {{ ref('latest_project_repos_stargaze_count') }} s
    on p.repo = s.repo left join {{ ref('latest_project_repos_watcher_count') }} w
    on p.repo = w.repo left join latest_four_week_change_repos_fork_count fork_change
    on p.repo = fork_change.repo left join latest_four_week_change_repos_stargaze_count stargaze_change
    on p.repo = stargaze_change.repo left join latest_four_week_change_repos_watcher_count watcher_change
    on p.repo = watcher_change.repo
),

metrics_with_overall_max_ts AS (
    SELECT
        am.*,
        MAX(row_data_timestamp) OVER () AS data_timestamp 
    FROM all_metrics am
),

normalized_metrics AS (
     SELECT
         repo,
         data_timestamp,
         fork_count,
         stargaze_count,
         watcher_count,
         fork_count_pct_change_over_4_weeks,
         stargaze_count_pct_change_over_4_weeks,
         watcher_count_pct_change_over_4_weeks,
         (fork_count - MIN(fork_count) OVER ())::NUMERIC / NULLIF((MAX(fork_count) OVER () - MIN(fork_count) OVER ())::NUMERIC,0) AS normalized_fork_count,
         (stargaze_count - MIN(stargaze_count) OVER ())::NUMERIC / NULLIF((MAX(stargaze_count) OVER () - MIN(stargaze_count) OVER ())::NUMERIC,0) AS normalized_stargaze_count,
         (watcher_count - MIN(watcher_count) OVER ())::NUMERIC / NULLIF((MAX(watcher_count) OVER () - MIN(watcher_count) OVER ())::NUMERIC,0) AS normalized_watcher_count,
         (fork_count_pct_change_over_4_weeks - MIN(fork_count_pct_change_over_4_weeks) OVER ())::NUMERIC / NULLIF((MAX(fork_count_pct_change_over_4_weeks) OVER () - MIN(fork_count_pct_change_over_4_weeks) OVER ())::NUMERIC,0) AS normalized_fork_count_pct_change_over_4_weeks,
         (stargaze_count_pct_change_over_4_weeks - MIN(stargaze_count_pct_change_over_4_weeks) OVER ())::NUMERIC / NULLIF((MAX(stargaze_count_pct_change_over_4_weeks) OVER () - MIN(stargaze_count_pct_change_over_4_weeks) OVER ())::NUMERIC,0) AS normalized_stargaze_count_pct_change_over_4_weeks,
         (watcher_count_pct_change_over_4_weeks - MIN(watcher_count_pct_change_over_4_weeks) OVER ())::NUMERIC / NULLIF((MAX(watcher_count_pct_change_over_4_weeks) OVER () - MIN(watcher_count_pct_change_over_4_weeks) OVER ())::NUMERIC,0) AS normalized_watcher_count_pct_change_over_4_weeks
     FROM metrics_with_overall_max_ts
),

ranked_projects AS (
  SELECT
      repo,
      data_timestamp,
      fork_count,
      stargaze_count,
      watcher_count,
      fork_count_pct_change_over_4_weeks,
      stargaze_count_pct_change_over_4_weeks,
      watcher_count_pct_change_over_4_weeks,
      normalized_fork_count,
      normalized_stargaze_count,
      normalized_watcher_count,
      normalized_fork_count_pct_change_over_4_weeks,
      normalized_stargaze_count_pct_change_over_4_weeks,
      normalized_watcher_count_pct_change_over_4_weeks,
        (
            (coalesce(normalized_fork_count,0) * 0.20) + 
            (coalesce(normalized_stargaze_count,0) * 0.20) + 
            (coalesce(normalized_watcher_count,0) * 0.1) + 
            (coalesce(normalized_fork_count_pct_change_over_4_weeks,0) * 0.20) + 
            (coalesce(normalized_stargaze_count_pct_change_over_4_weeks,0) * 0.20) +
            (coalesce(normalized_watcher_count_pct_change_over_4_weeks,0) * 0.1)
        ) AS weighted_score

  FROM normalized_metrics
), 

final_ranking AS (
  SELECT
    repo,
    data_timestamp,
    fork_count,
    stargaze_count,
    watcher_count,
    fork_count_pct_change_over_4_weeks,
    stargaze_count_pct_change_over_4_weeks,
    watcher_count_pct_change_over_4_weeks,
    normalized_fork_count,
    normalized_stargaze_count,
    normalized_watcher_count,
    normalized_fork_count_pct_change_over_4_weeks,
    normalized_stargaze_count_pct_change_over_4_weeks,
    normalized_watcher_count_pct_change_over_4_weeks,
    weighted_score,
    RANK() OVER (
      ORDER BY weighted_score DESC
    ) AS repo_rank,
    NTILE(4) OVER (
      ORDER BY weighted_score DESC
    ) AS quartile_bucket

  FROM ranked_projects
)

SELECT
  repo,
  data_timestamp,
  fork_count::integer,
  stargaze_count::integer,
  watcher_count::integer,
  fork_count_pct_change_over_4_weeks::numeric,
  stargaze_count_pct_change_over_4_weeks::numeric,
  watcher_count_pct_change_over_4_weeks::numeric,
  normalized_fork_count::numeric,
  normalized_stargaze_count::numeric,
  normalized_watcher_count::numeric,
  normalized_fork_count_pct_change_over_4_weeks::numeric,
  normalized_stargaze_count_pct_change_over_4_weeks::numeric,
  normalized_watcher_count_pct_change_over_4_weeks::numeric,
  weighted_score::numeric,
  round(weighted_score * 100, 1) AS weighted_score_index,
  repo_rank,
  quartile_bucket,
  CASE
      WHEN quartile_bucket = 1 THEN 'Top Repo'
      WHEN quartile_bucket = 2 THEN 'Leader'
      WHEN quartile_bucket = 3 THEN 'In-The-Mix'
      WHEN quartile_bucket = 4 THEN 'Laggard'
      ELSE 'Unknown'
  END AS repo_rank_category

FROM final_ranking

ORDER BY weighted_score DESC