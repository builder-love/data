-- models/clean/latest_top_projects.sql
-- calculate top_project score
-- metrics and weights include: 
--   - total all-time commit count (.15), total all-time fork count (.15), total all-time stargaze count (.15), total all-time contributor count (.15)
--   - one month change commit count (.10), one month change fork count (.10), one month change stargaze count (.10), one month change contributor count (.10)

{{ config(
    materialized='table',
    unique_key='project_title || data_timestamp',
    tags=['latest_clean_data']
) }}

with all_metrics as (
  select 
    p.project_title,
    GREATEST(p.data_timestamp, f.data_timestamp, s.data_timestamp, cc.data_timestamp, c.data_timestamp, commit_change.data_timestamp, contributor_change.data_timestamp, fork_change.data_timestamp, stargaze_change.data_timestamp) row_data_timestamp,
    COALESCE(f.fork_count,0) fork_count,
    COALESCE(s.stargaze_count,0) stargaze_count,
    COALESCE(cc.commit_count,0) commit_count,
    COALESCE(c.contributor_count,0) contributor_count,
    COALESCE(commit_change.commit_count_pct_change_over_4_weeks,0) commit_count_pct_change_over_4_weeks,
    COALESCE(contributor_change.contributor_count_pct_change_over_4_weeks,0) contributor_count_pct_change_over_4_weeks,
    COALESCE(fork_change.fork_count_pct_change_over_4_weeks,0) fork_count_pct_change_over_4_weeks,
    COALESCE(stargaze_change.stargaze_count_pct_change_over_4_weeks,0) stargaze_count_pct_change_over_4_weeks
    
  from {{ ref('latest_distinct_projects') }} p left join {{ ref('latest_project_fork_count') }} f 
    on p.project_title = f.project_title left join {{ ref('latest_project_stargaze_count') }} s
    on p.project_title = s.project_title left join {{ ref('latest_project_commit_count') }} cc
    on p.project_title = cc.project_title left join {{ ref('latest_project_contributor_count') }} c
    on p.project_title = c.project_title left join {{ ref('four_week_change_project_commit_count') }} commit_change
    on p.project_title = commit_change.project_title left join {{ ref('four_week_change_project_contributor_count') }} contributor_change
    on p.project_title = contributor_change.project_title left join {{ ref('four_week_change_project_fork_count') }} fork_change
    on p.project_title = fork_change.project_title left join {{ ref('four_week_change_project_stargaze_count') }} stargaze_change
    on p.project_title = stargaze_change.project_title
),

metrics_with_overall_max_ts AS (
    SELECT
        am.*,
        MAX(row_data_timestamp) OVER () AS data_timestamp 
    FROM all_metrics am
),

normalized_metrics AS (
     SELECT
         project_title,
         row_data_timestamp,
         data_timestamp,
         fork_count,
         stargaze_count,
         commit_count,
         contributor_count,
         commit_count_pct_change_over_4_weeks,
         contributor_count_pct_change_over_4_weeks,
         fork_count_pct_change_over_4_weeks,
         stargaze_count_pct_change_over_4_weeks,
         (fork_count - MIN(fork_count) OVER ())::NUMERIC / NULLIF((MAX(fork_count) OVER () - MIN(fork_count) OVER ())::NUMERIC,0) AS normalized_fork_count,
         (stargaze_count - MIN(stargaze_count) OVER ())::NUMERIC / NULLIF((MAX(stargaze_count) OVER () - MIN(stargaze_count) OVER ())::NUMERIC,0) AS normalized_stargaze_count,
         (commit_count - MIN(commit_count) OVER ())::NUMERIC / NULLIF((MAX(commit_count) OVER () - MIN(commit_count) OVER ())::NUMERIC,0) AS normalized_commit_count,
         (contributor_count - MIN(contributor_count) OVER ())::NUMERIC / NULLIF((MAX(contributor_count) OVER () - MIN(contributor_count) OVER ())::NUMERIC,0) AS normalized_contributor_count,
         (commit_count_pct_change_over_4_weeks - MIN(commit_count_pct_change_over_4_weeks) OVER ())::NUMERIC / NULLIF((MAX(commit_count_pct_change_over_4_weeks) OVER () - MIN(commit_count_pct_change_over_4_weeks) OVER ())::NUMERIC,0) AS normalized_commit_count_pct_change_over_4_weeks,
         (contributor_count_pct_change_over_4_weeks - MIN(contributor_count_pct_change_over_4_weeks) OVER ())::NUMERIC / NULLIF((MAX(contributor_count_pct_change_over_4_weeks) OVER () - MIN(contributor_count_pct_change_over_4_weeks) OVER ())::NUMERIC,0) AS normalized_contributor_count_pct_change_over_4_weeks,
         (fork_count_pct_change_over_4_weeks - MIN(fork_count_pct_change_over_4_weeks) OVER ())::NUMERIC / NULLIF((MAX(fork_count_pct_change_over_4_weeks) OVER () - MIN(fork_count_pct_change_over_4_weeks) OVER ())::NUMERIC,0) AS normalized_fork_count_pct_change_over_4_weeks,
         (stargaze_count_pct_change_over_4_weeks - MIN(stargaze_count_pct_change_over_4_weeks) OVER ())::NUMERIC / NULLIF((MAX(stargaze_count_pct_change_over_4_weeks) OVER () - MIN(stargaze_count_pct_change_over_4_weeks) OVER ())::NUMERIC,0) AS normalized_stargaze_count_pct_change_over_4_weeks
     FROM metrics_with_overall_max_ts
     WHERE project_title IS NOT NULL
),

ranked_projects AS (
  SELECT
      project_title,
      row_data_timestamp,
      data_timestamp,
      fork_count,
      stargaze_count,
      commit_count,
      contributor_count,
      commit_count_pct_change_over_4_weeks,
      contributor_count_pct_change_over_4_weeks,
      fork_count_pct_change_over_4_weeks,
      stargaze_count_pct_change_over_4_weeks,
      normalized_fork_count,
      normalized_stargaze_count,
      normalized_commit_count,
      normalized_contributor_count,
      normalized_commit_count_pct_change_over_4_weeks,
      normalized_contributor_count_pct_change_over_4_weeks,
      normalized_fork_count_pct_change_over_4_weeks,
      normalized_stargaze_count_pct_change_over_4_weeks,
    (
      (coalesce(normalized_fork_count,0) * 0.15) + 
      (coalesce(normalized_stargaze_count,0) * 0.15) + 
      (coalesce(normalized_commit_count,0) * 0.15) + 
      (coalesce(normalized_contributor_count,0) * 0.15) + 
      (coalesce(normalized_commit_count_pct_change_over_4_weeks,0) * 0.10) + 
      (coalesce(normalized_contributor_count_pct_change_over_4_weeks,0) * 0.10) + 
      (coalesce(normalized_fork_count_pct_change_over_4_weeks,0) * 0.10) + 
      (coalesce(normalized_stargaze_count_pct_change_over_4_weeks,0) * 0.10)
    ) AS weighted_score

  FROM normalized_metrics
), 

final_ranking AS (
  SELECT
    project_title,
    row_data_timestamp,
    data_timestamp,
    fork_count,
    stargaze_count,
    commit_count,
    contributor_count,
    commit_count_pct_change_over_4_weeks,
    contributor_count_pct_change_over_4_weeks,
    fork_count_pct_change_over_4_weeks,
    stargaze_count_pct_change_over_4_weeks,
    normalized_fork_count,
    normalized_stargaze_count,
    normalized_commit_count,
    normalized_contributor_count,
    normalized_commit_count_pct_change_over_4_weeks,
    normalized_contributor_count_pct_change_over_4_weeks,
    normalized_fork_count_pct_change_over_4_weeks,
    normalized_stargaze_count_pct_change_over_4_weeks,
    weighted_score,
    RANK() OVER (
            ORDER BY
                weighted_score DESC
    ) AS project_rank,
    NTILE(4) OVER (
        ORDER BY
            weighted_score DESC
    ) AS quartile_bucket

  FROM ranked_projects
)

SELECT
  project_title,
  row_data_timestamp,
  data_timestamp,
  fork_count,
  stargaze_count,
  commit_count,
  contributor_count,
  commit_count_pct_change_over_4_weeks,
  contributor_count_pct_change_over_4_weeks,
  fork_count_pct_change_over_4_weeks,
  stargaze_count_pct_change_over_4_weeks,
  normalized_fork_count,
  normalized_stargaze_count,
  normalized_commit_count,
  normalized_contributor_count,
  normalized_commit_count_pct_change_over_4_weeks,
  normalized_contributor_count_pct_change_over_4_weeks,
  normalized_fork_count_pct_change_over_4_weeks,
  normalized_stargaze_count_pct_change_over_4_weeks,
  weighted_score,
  project_rank,
  quartile_bucket,
  CASE
      WHEN quartile_bucket = 1 THEN 'Top Project'
      WHEN quartile_bucket = 2 THEN 'Leader'
      WHEN quartile_bucket = 3 THEN 'In-The-Mix'
      WHEN quartile_bucket = 4 THEN 'Laggard'
      ELSE 'Unknown'
  END AS project_rank_category

FROM final_ranking

ORDER BY weighted_score desc
