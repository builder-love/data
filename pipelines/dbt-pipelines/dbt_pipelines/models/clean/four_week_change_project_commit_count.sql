-- models/clean/four_week_change_project_commit_count.sql
-- this table updates weekly, so we take a shortcut and lag 3 records to get the 4 week change

{{ 
    config(
        materialized='view',
        unique_key='project_title || data_timestamp',
        tags=['period_change_data']
    ) 
}} 

WITH LaggedWeeklyCounts AS (
    -- Step 3: Use LAG(..., 3) to get the value from 4 weeks prior
    SELECT
        project_title,
        data_timestamp,
        commit_count,
        LAG(commit_count, 3) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_commit_count,
        LAG(data_timestamp, 3) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_timestamp -- The timestamp for the prior count
    FROM 
        {{ ref('normalized_project_commit_count') }}
    where commit_count IS NOT NULL
),

-- Step 4: Final Calculation
change_over_4_weeks as (
    SELECT
        project_title,
        data_timestamp,
        commit_count AS current_commits,
        prior_4_weeks_timestamp, 
        prior_4_weeks_commit_count,
        (commit_count - prior_4_weeks_commit_count) AS commit_count_change_over_4_weeks,
        (commit_count::NUMERIC/nullif(prior_4_weeks_commit_count::NUMERIC, 0)) - 1 as commit_count_pct_change_over_4_weeks
    FROM 
        LaggedWeeklyCounts
    WHERE 
        prior_4_weeks_commit_count IS NOT NULL -- Only show rows where a 4-week prior value exists
)

SELECT 
    project_title, 
    (data_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS data_timestamp, 
    current_commits, 
    (prior_4_weeks_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS prior_4_weeks_timestamp,
    prior_4_weeks_commit_count, 
    commit_count_change_over_4_weeks, 
    commit_count_pct_change_over_4_weeks
FROM change_over_4_weeks
