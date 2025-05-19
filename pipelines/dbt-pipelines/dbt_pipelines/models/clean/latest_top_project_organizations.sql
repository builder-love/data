-- models/clean/latest_top_project_organizations.sql
-- calculate top_project_organizations score
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
    unique_key='project_organization_url || report_date',
    tags=['latest_clean_data']
) }}

WITH distinct_project_orgs as (
  select distinct 
    repo,
    project_organization_url
  
  from {{ ref('latest_project_repos') }}
  where project_organization_url <> 'https://github.com/'
),

four_week_ago_repo_fork_count as (
    select repo, data_timestamp, prior_4_weeks_fork_count
    from {{ ref('four_week_change_project_repos_fork_count') }}
    where data_timestamp = (select max(data_timestamp) from {{ ref('four_week_change_project_repos_fork_count') }})
),

four_week_ago_repo_stargaze_count as (
    select repo, data_timestamp, prior_4_weeks_stargaze_count
    from {{ ref('four_week_change_project_repos_stargaze_count') }}
    where data_timestamp = (select max(data_timestamp) from {{ ref('four_week_change_project_repos_stargaze_count') }})
),

four_week_ago_repo_watcher_count as (
    select repo, data_timestamp, prior_4_weeks_watcher_count
    from {{ ref('four_week_change_project_repos_watcher_count') }}
    where data_timestamp = (select max(data_timestamp) from {{ ref('four_week_change_project_repos_watcher_count') }})
),

all_metrics as (
  select 
    p.repo,
    p.project_organization_url,
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
    fork_change.prior_4_weeks_fork_count,
    stargaze_change.prior_4_weeks_stargaze_count,
    watcher_change.prior_4_weeks_watcher_count
    
  from distinct_project_orgs p left join {{ ref('latest_project_repos_fork_count') }} f 
    on p.repo = f.repo left join {{ ref('latest_project_repos_stargaze_count') }} s
    on p.repo = s.repo left join {{ ref('latest_project_repos_watcher_count') }} w
    on p.repo = w.repo left join four_week_ago_repo_fork_count fork_change
    on p.repo = fork_change.repo left join four_week_ago_repo_stargaze_count stargaze_change
    on p.repo = stargaze_change.repo left join four_week_ago_repo_watcher_count watcher_change
    on p.repo = watcher_change.repo
),

metrics_with_overall_max_ts AS (
    SELECT
        am.*,
        MAX(row_data_timestamp) OVER () AS data_timestamp 
    FROM all_metrics am
),

aggregate_metrics as (
  select 
    project_organization_url,
    max(data_timestamp) data_timestamp,
    count(repo) repo_count,
    sum(fork_count) fork_count,
    sum(stargaze_count) stargaze_count,
    sum(watcher_count) watcher_count,
    sum(prior_4_weeks_fork_count) prior_4_weeks_fork_count,
    sum(prior_4_weeks_stargaze_count) prior_4_weeks_stargaze_count,
    sum(prior_4_weeks_watcher_count) prior_4_weeks_watcher_count
  from metrics_with_overall_max_ts
  group by 1
),

aggregate_metrics_with_change as (
  SELECT
      project_organization_url,
      data_timestamp,
      fork_count,
      stargaze_count,
      watcher_count,
      ((fork_count::numeric/nullif(prior_4_weeks_fork_count::numeric,0)) - 1) fork_count_pct_change_over_4_weeks,
      ((stargaze_count::numeric/nullif(prior_4_weeks_stargaze_count::numeric,0)) - 1) stargaze_count_pct_change_over_4_weeks,
      ((watcher_count::numeric/nullif(prior_4_weeks_watcher_count::numeric,0)) - 1) watcher_count_pct_change_over_4_weeks
  from aggregate_metrics 
),

normalized_metrics AS (
  SELECT
    project_organization_url,
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
     FROM aggregate_metrics_with_change
),

ranked_projects AS (
  SELECT
      project_organization_url,
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
    project_organization_url,
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
    ) AS org_rank,
    NTILE(4) OVER (
      ORDER BY weighted_score DESC
    ) AS quartile_bucket

  FROM ranked_projects
)

SELECT
  project_organization_url,
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
  org_rank,
  quartile_bucket,
  CASE
      WHEN quartile_bucket = 1 THEN 'Top Repo'
      WHEN quartile_bucket = 2 THEN 'Leader'
      WHEN quartile_bucket = 3 THEN 'In-The-Mix'
      WHEN quartile_bucket = 4 THEN 'Laggard'
      ELSE 'Unknown'
  END AS org_rank_category

FROM final_ranking

ORDER BY weighted_score DESC



