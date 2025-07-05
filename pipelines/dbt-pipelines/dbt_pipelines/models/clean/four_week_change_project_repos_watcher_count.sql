-- models/clean/four_week_change_project_repos_watcher_count.sql
-- this table updates weekly, so we take a shortcut and lag 3 records to get the 4 week change

{{ 
    config(
        materialized='view',
        unique_key='repo || data_timestamp',
        tags=['period_change_data']
    ) 
}} 

WITH LaggedWeeklyCounts AS (
    -- Use LAG(..., 3) to get the value from 4 weeks prior
    SELECT
        repo,
        data_timestamp,
        watcher_count,
        LAG(watcher_count, 3) OVER (
            PARTITION BY repo 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_watcher_count,
        LAG(data_timestamp, 3) OVER (
            PARTITION BY repo 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_timestamp -- The timestamp for the prior count
    FROM 
        {{ ref('normalized_project_repos_watcher_count') }}
    where watcher_count IS NOT NULL
),

change_over_4_weeks as (
    SELECT
        repo,
        data_timestamp,
        watcher_count AS current_watchers,
        prior_4_weeks_timestamp, 
        prior_4_weeks_watcher_count,
        (watcher_count - prior_4_weeks_watcher_count) AS watcher_count_change_over_4_weeks,
        (watcher_count::NUMERIC/nullif(prior_4_weeks_watcher_count::NUMERIC, 0)) - 1 as watcher_count_pct_change_over_4_weeks
    FROM 
        LaggedWeeklyCounts
    WHERE 
        prior_4_weeks_watcher_count IS NOT NULL -- Only show rows where a 4-week prior value exists
)

SELECT 
    repo, 
    (data_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS data_timestamp, 
    current_watchers::BIGINT, 
    (prior_4_weeks_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS prior_4_weeks_timestamp,
    prior_4_weeks_watcher_count::BIGINT, 
    watcher_count_change_over_4_weeks::INTEGER, 
    watcher_count_pct_change_over_4_weeks::NUMERIC
FROM change_over_4_weeks
