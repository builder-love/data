-- models/clean/four_week_change_project_watcher_count.sql
-- this table updates weekly, so we take a shortcut and lag 4 records to get the 4 week change

{{ 
    config(
        materialized='view',
        unique_key='project_title',
        tags=['period_change_data']
    ) 
}} 

WITH LaggedWeeklyCounts AS (
    -- Use LAG(..., 4) to get the value from 4 weeks prior
    SELECT
        project_title,
        data_timestamp,
        watcher_count,
        LAG(watcher_count, 4) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_watcher_count,
        LAG(data_timestamp, 4) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_timestamp -- The timestamp for the prior count
    FROM 
        {{ ref('normalized_project_watcher_count') }}
    where watcher_count IS NOT NULL
),

change_over_4_weeks as (
    SELECT
        project_title,
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
    project_title, 
    (data_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS data_timestamp, 
    current_watchers, 
    (prior_4_weeks_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS prior_4_weeks_timestamp,
    prior_4_weeks_watcher_count, 
    watcher_count_change_over_4_weeks, 
    watcher_count_pct_change_over_4_weeks
FROM change_over_4_weeks
