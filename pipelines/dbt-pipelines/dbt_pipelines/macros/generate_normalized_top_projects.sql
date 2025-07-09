-- macros/generate_normalized_top_projects.sql

{% macro generate_normalized_top_projects(
    commit_count_model, 
    contributor_count_model,
    commit_change_model,
    contributor_change_model,
    metric_weights={}
) %}

{#- Define the default weights -#}
{% set default_weights = {
    'fork_count': 0.15,
    'stargaze_count': 0.15,
    'commit_count': 0.15,
    'contributor_count': 0.15,
    'watcher_count': 0.125,
    'is_not_fork_ratio': 0.125,
    'commit_count_pct_change': 0.025,
    'contributor_count_pct_change': 0.025,
    'fork_count_pct_change': 0.025,
    'stargaze_count_pct_change': 0.025,
    'watcher_count_pct_change': 0.025,
    'is_not_fork_ratio_pct_change': 0.025
} %}

{#- Update the defaults with any weights passed to the macro -#}
{% set final_weights = default_weights.copy() %}
{% do final_weights.update(metric_weights) %}

-- rm from project list:
-- General
-- Ethereum L2s
-- Ethereum Virtual Machine Stack
-- EVM Compatible L1 and L2
-- EVM Compatible Layer 1s
-- EVM Compatible Layer 1s, 2s, and Dapps
-- EVM Compatible Layer 2s
-- Solana Virtual Machine Stack
-- SVM Layer 1 and Layer 2s
-- SVM Layer 1s
-- Open Source Cryptography
-- ....

WITH project_date_range AS (
    -- Determine the overall start and end dates for tables
    SELECT
       '2025-01-01'::date AS min_start_date,
       CURRENT_DATE AS max_end_date
),
-- Generate all dates within the calculated range
all_dates_in_range AS (
    SELECT generate_series(
        (SELECT min_start_date FROM project_date_range),
        (SELECT max_end_date FROM project_date_range),
        '1 day'::interval
    )::date AS dt
),
-- Filter the generated dates to keep only Sundays
date_series AS (
    SELECT dt AS report_date
    FROM all_dates_in_range
    WHERE EXTRACT(ISODOW FROM dt) = 7
),

-- create the scaffold
project_date_series_scaffold as (
  SELECT
    project_title,
    report_date

  FROM {{ ref('latest_distinct_projects') }} cross join date_series -- get the full cartesian product

WHERE LOWER(project_title) NOT IN (
    'general',
    'ethereum l2s',
    'ethereum virtual machine stack',
    'evm compatible l1 and l2',
    'evm compatible layer 1s',
    'evm compatible layer 1s, 2s, and dapps',
    'evm compatible layer 2s',
    'solana virtual machine stack',
    'svm layer 1 and layer 2s',
    'svm layer 1s',
    'open source cryptography',
    'multichain infrastructure',
    'evm multichain dapps',
    'wallet (category)',
    'bridges & interoperability (category)',
    'oracles, data feeds, data providers (category)',
    'oracles (category)',
    'evm compatible application',
    'zero knowledge cryptography',
    'bridge (category)',
    'cosmos network stack',
    'polkadot network stack',
    'evm toolkit',
    'move stack',
    'solidity'
    )
and report_date >= (CURRENT_DATE - INTERVAL '52 weeks')
),

-- add the sunday report_date to each source table
normalized_project_fork_count as (
select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
from {{ ref('normalized_project_fork_count') }} f
where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
normalized_project_stargaze_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref('normalized_project_stargaze_count') }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
normalized_project_commit_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref(commit_count_model) }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
normalized_project_contributor_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref(contributor_count_model) }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
normalized_project_watcher_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref('normalized_project_watcher_count') }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
normalized_project_is_fork as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref('normalized_project_is_fork') }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
four_week_change_project_commit_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref(commit_change_model) }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
four_week_change_project_contributor_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref(contributor_change_model) }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
four_week_change_project_fork_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref('four_week_change_project_fork_count') }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
four_week_change_project_stargaze_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref('four_week_change_project_stargaze_count') }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
four_week_change_project_watcher_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref('four_week_change_project_watcher_count') }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
), 
four_week_change_project_is_fork as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref('four_week_change_project_is_fork') }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
),
four_week_change_project_repo_count as (
  select f.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref('four_week_change_project_repo_count') }} f
  where data_timestamp >= (CURRENT_DATE - INTERVAL '52 weeks')
),
-- pass repo count as a reference for the chart
normalized_project_repo_count as (
  select 
    r.*, (data_timestamp - (EXTRACT(ISODOW FROM data_timestamp) - 7) * interval '1 day')::date report_date
  from {{ ref('normalized_project_repo_count') }} r
),

all_metrics as (
  select 
    p.project_title,
    p.report_date,
    f.data_timestamp as fork_data_timestamp, 
    s.data_timestamp as stargaze_data_timestamp, 
    cc.data_timestamp as commit_data_timestamp, 
    c.data_timestamp as contributor_data_timestamp, 
    w.data_timestamp as watcher_data_timestamp, 
    is_fork.data_timestamp as is_fork_data_timestamp, 
    commit_change.data_timestamp as commit_change_data_timestamp, 
    contributor_change.data_timestamp as contributor_change_data_timestamp, 
    fork_change.data_timestamp as fork_change_data_timestamp, 
    stargaze_change.data_timestamp as stargaze_change_data_timestamp,
    watcher_change.data_timestamp as watcher_change_data_timestamp,
    is_fork_change.data_timestamp as is_fork_change_data_timestamp,
    r.data_timestamp as repo_data_timestamp,
    repo_change.data_timestamp as repo_change_data_timestamp,
    GREATEST(f.data_timestamp, s.data_timestamp, cc.data_timestamp, c.data_timestamp, w.data_timestamp, is_fork.data_timestamp, commit_change.data_timestamp, contributor_change.data_timestamp, fork_change.data_timestamp, stargaze_change.data_timestamp, watcher_change.data_timestamp,
    is_fork_change.data_timestamp, repo_change.data_timestamp, r.data_timestamp) row_data_timestamp,
    f.fork_count,
    s.stargaze_count,
    cc.commit_count,
    c.contributor_count,
    w.watcher_count,
    is_fork.is_not_fork_ratio,
    commit_change.commit_count_pct_change_over_4_weeks,
    contributor_change.contributor_count_pct_change_over_4_weeks,
    fork_change.fork_count_pct_change_over_4_weeks,
    stargaze_change.stargaze_count_pct_change_over_4_weeks,
    watcher_change.watcher_count_pct_change_over_4_weeks,
    is_fork_change.is_not_fork_ratio_pct_change_over_4_weeks,
    repo_change.repo_count_pct_change_over_4_weeks,
    r.repo_count

  from project_date_series_scaffold p left join normalized_project_fork_count f 
    on p.project_title = f.project_title and p.report_date = f.report_date left join normalized_project_stargaze_count s
    on p.project_title = s.project_title and p.report_date = s.report_date left join normalized_project_commit_count cc
    on p.project_title = cc.project_title and p.report_date = cc.report_date left join normalized_project_contributor_count c
    on p.project_title = c.project_title and p.report_date = c.report_date left join normalized_project_watcher_count w
    on p.project_title = w.project_title and p.report_date = w.report_date left join normalized_project_is_fork is_fork
    on p.project_title = is_fork.project_title and p.report_date = is_fork.report_date left join four_week_change_project_commit_count commit_change
    on p.project_title = commit_change.project_title and p.report_date = commit_change.report_date left join four_week_change_project_contributor_count contributor_change
    on p.project_title = contributor_change.project_title and p.report_date = contributor_change.report_date left join four_week_change_project_fork_count fork_change
    on p.project_title = fork_change.project_title and p.report_date = fork_change.report_date left join four_week_change_project_stargaze_count stargaze_change
    on p.project_title = stargaze_change.project_title and p.report_date = stargaze_change.report_date left join four_week_change_project_watcher_count watcher_change
    on p.project_title = watcher_change.project_title and p.report_date = watcher_change.report_date left join four_week_change_project_is_fork is_fork_change
    on p.project_title = is_fork_change.project_title and p.report_date = is_fork_change.report_date left join normalized_project_repo_count r
    on p.project_title = r.project_title and p.report_date = r.report_date left join four_week_change_project_repo_count repo_change
    on p.project_title = repo_change.project_title and p.report_date = repo_change.report_date
),

metrics_with_overall_max_ts AS (
    SELECT
        am.*,
        MAX(row_data_timestamp) OVER () AS data_timestamp 
    FROM all_metrics am
),

-- create grouping identifiers for each column needing LOCF
grouped_metrics AS (
    SELECT
        *,
        -- Create grouping keys - these increment only when a non-null value appears for THAT column
        -- metrics
        COUNT(repo_count) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as repo_grp,
        COUNT(fork_count) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as fork_grp,
        COUNT(stargaze_count) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as stargaze_grp,
        COUNT(commit_count) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as commit_grp,
        COUNT(contributor_count) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as contributor_grp,
        COUNT(watcher_count) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as watcher_grp,
        COUNT(is_not_fork_ratio) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as is_not_fork_ratio_grp,
        COUNT(commit_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as commit_change_grp,
        COUNT(contributor_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as contributor_change_grp,
        COUNT(fork_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as fork_change_grp,
        COUNT(stargaze_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as stargaze_change_grp,
        COUNT(watcher_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as watcher_change_grp,
        COUNT(is_not_fork_ratio_pct_change_over_4_weeks) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as is_fork_ratio_change_grp,
        COUNT(repo_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title ORDER BY report_date ROWS UNBOUNDED PRECEDING) as repo_change_grp
    FROM
        metrics_with_overall_max_ts -- Use the joined data before trying LOCF
),

-- propagate the last known value within each group
metrics_locf AS (
    SELECT
        project_title,
        report_date,
        data_timestamp,
        -- metrics
        MAX(repo_count) OVER (PARTITION BY project_title, repo_grp) AS repo_count,
        MAX(fork_count) OVER (PARTITION BY project_title, fork_grp) AS fork_count,
        MAX(stargaze_count) OVER (PARTITION BY project_title, stargaze_grp) AS stargaze_count,
        MAX(commit_count) OVER (PARTITION BY project_title, commit_grp) AS commit_count,
        MAX(contributor_count) OVER (PARTITION BY project_title, contributor_grp) AS contributor_count,
        MAX(watcher_count) OVER (PARTITION BY project_title, watcher_grp) AS watcher_count,
        MAX(is_not_fork_ratio) OVER (PARTITION BY project_title, is_not_fork_ratio_grp) AS is_not_fork_ratio,
        MAX(commit_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title, commit_change_grp) AS commit_count_pct_change_over_4_weeks,
        MAX(contributor_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title, contributor_change_grp) AS contributor_count_pct_change_over_4_weeks,
        MAX(fork_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title, fork_change_grp) AS fork_count_pct_change_over_4_weeks,
        MAX(stargaze_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title, stargaze_change_grp) AS stargaze_count_pct_change_over_4_weeks,
        MAX(watcher_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title, watcher_change_grp) AS watcher_count_pct_change_over_4_weeks,
        MAX(is_not_fork_ratio_pct_change_over_4_weeks) OVER (PARTITION BY project_title, is_fork_ratio_change_grp) AS is_not_fork_ratio_pct_change_over_4_weeks,
        MAX(repo_count_pct_change_over_4_weeks) OVER (PARTITION BY project_title, repo_change_grp) AS repo_count_pct_change_over_4_weeks
    FROM
        grouped_metrics
),

normalized_metrics AS (
     SELECT
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
         (fork_count - MIN(fork_count) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(fork_count) OVER (PARTITION BY report_date) - MIN(fork_count) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_fork_count,
         (stargaze_count - MIN(stargaze_count) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(stargaze_count) OVER (PARTITION BY report_date) - MIN(stargaze_count) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_stargaze_count,
         (commit_count - MIN(commit_count) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(commit_count) OVER (PARTITION BY report_date) - MIN(commit_count) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_commit_count,
         (contributor_count - MIN(contributor_count) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(contributor_count) OVER (PARTITION BY report_date) - MIN(contributor_count) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_contributor_count,
         (watcher_count - MIN(watcher_count) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(watcher_count) OVER (PARTITION BY report_date) - MIN(watcher_count) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_watcher_count,
         (is_not_fork_ratio - MIN(is_not_fork_ratio) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(is_not_fork_ratio) OVER (PARTITION BY report_date) - MIN(is_not_fork_ratio) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_is_not_fork_ratio, 
         (commit_count_pct_change_over_4_weeks - MIN(commit_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(commit_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date) - MIN(commit_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_commit_count_pct_change_over_4_weeks,
         (contributor_count_pct_change_over_4_weeks - MIN(contributor_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(contributor_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date) - MIN(contributor_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_contributor_count_pct_change_over_4_weeks,
         (fork_count_pct_change_over_4_weeks - MIN(fork_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(fork_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date) - MIN(fork_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_fork_count_pct_change_over_4_weeks,
         (stargaze_count_pct_change_over_4_weeks - MIN(stargaze_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(stargaze_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date) - MIN(stargaze_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_stargaze_count_pct_change_over_4_weeks,
         (watcher_count_pct_change_over_4_weeks - MIN(watcher_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(watcher_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date) - MIN(watcher_count_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_watcher_count_pct_change_over_4_weeks,
         (is_not_fork_ratio_pct_change_over_4_weeks - MIN(is_not_fork_ratio_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC / NULLIF((MAX(is_not_fork_ratio_pct_change_over_4_weeks) OVER (PARTITION BY report_date) - MIN(is_not_fork_ratio_pct_change_over_4_weeks) OVER (PARTITION BY report_date))::NUMERIC,0) AS normalized_is_not_fork_ratio_pct_change_over_4_weeks 
     FROM metrics_locf
     WHERE project_title IS NOT NULL and report_date > '3/1/2025' -- there is one contributor count record prior to march 2025
),

ranked_projects AS (
  SELECT
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
    (
      (coalesce(normalized_fork_count,0) * {{ final_weights.fork_count }}) + 
      (coalesce(normalized_stargaze_count,0) * {{ final_weights.stargaze_count }}) + 
      (coalesce(normalized_commit_count,0) * {{ final_weights.commit_count }}) + 
      (coalesce(normalized_contributor_count,0) * {{ final_weights.contributor_count }}) + 
      (coalesce(normalized_watcher_count,0) * {{ final_weights.watcher_count }}) + 
      (coalesce(normalized_is_not_fork_ratio,0) * {{ final_weights.is_not_fork_ratio }}) + 
      (coalesce(normalized_commit_count_pct_change_over_4_weeks,0) * {{ final_weights.commit_count_pct_change }}) + 
      (coalesce(normalized_contributor_count_pct_change_over_4_weeks,0) * {{ final_weights.contributor_count_pct_change }}) + 
      (coalesce(normalized_fork_count_pct_change_over_4_weeks,0) * {{ final_weights.fork_count_pct_change }}) + 
      (coalesce(normalized_stargaze_count_pct_change_over_4_weeks,0) * {{ final_weights.stargaze_count_pct_change }}) +
      (coalesce(normalized_watcher_count_pct_change_over_4_weeks,0) * {{ final_weights.watcher_count_pct_change }}) +
      (coalesce(normalized_is_not_fork_ratio_pct_change_over_4_weeks,0) * {{ final_weights.is_not_fork_ratio_pct_change }})
    ) AS weighted_score

  FROM normalized_metrics
), 

final_ranking AS (
  SELECT
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
    RANK() OVER (
      PARTITION BY report_date
      ORDER BY weighted_score DESC
    ) AS project_rank,
    NTILE(4) OVER (
      PARTITION BY report_date
      ORDER BY weighted_score DESC
    ) AS quartile_bucket,
    -- Calculate the 8-week simple moving average (current week + 7 preceding weeks)
    AVG(weighted_score) OVER (
        PARTITION BY project_title 
        ORDER BY report_date ASC   
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW -- Window: current row + 7 previous rows
    ) AS weighted_score_sma,
    LAG(weighted_score, 3) OVER (
        PARTITION BY project_title 
        ORDER BY report_date
    ) AS prior_4_weeks_weighted_score

  FROM ranked_projects
),

ranking_changes as (
  select fr.*,
  LAG(project_rank, 3) OVER (
      PARTITION BY project_title 
      ORDER BY report_date
  ) AS prior_4_weeks_project_rank

  from final_ranking fr
)

SELECT
  project_title,
  report_date::timestamp AS report_date,
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
  weighted_score_sma,
  prior_4_weeks_weighted_score,
  round(weighted_score * 100, 1) AS weighted_score_index,
  project_rank,
  prior_4_weeks_project_rank,
  (prior_4_weeks_project_rank - project_rank) absolute_project_rank_change_over_4_weeks,
  RANK() OVER (
    ORDER BY (prior_4_weeks_project_rank - project_rank) DESC
  ) AS rank_of_project_rank_change_over_4_weeks,
  quartile_bucket,
  CASE
      WHEN quartile_bucket = 1 THEN 'Top Project'
      WHEN quartile_bucket = 2 THEN 'Leader'
      WHEN quartile_bucket = 3 THEN 'In-The-Mix'
      WHEN quartile_bucket = 4 THEN 'Laggard'
      ELSE 'Unknown'
  END AS project_rank_category

FROM ranking_changes

ORDER BY report_date desc, weighted_score DESC

{% endmacro %}