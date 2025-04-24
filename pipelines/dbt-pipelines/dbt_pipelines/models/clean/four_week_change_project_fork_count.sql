-- models/clean/four_week_change_project_fork_count.sql
-- this table updates weekly, so we take a shortcut and lag 4 records to get the 4 week change

{{ 
    config(
        materialized='view',
        unique_key='project_title',
        tags=['period_change_data']
    ) 
}} 

WITH LaggedWeeklyCounts AS (
    -- Step 3: Use LAG(..., 4) to get the value from 4 weeks prior
    SELECT
        project_title,
        data_timestamp,
        fork_count,
        LAG(fork_count, 4) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_fork_count,
        LAG(data_timestamp, 4) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_timestamp -- The timestamp for the prior count
    FROM 
        {{ ref('normalized_project_fork_count') }}
    where fork_count IS NOT NULL
),

-- Step 4: Final Calculation
change_over_4_weeks as (
    SELECT
        project_title,
        data_timestamp,
        fork_count AS current_forks,
        prior_4_weeks_timestamp, 
        prior_4_weeks_fork_count,
        (fork_count - prior_4_weeks_fork_count) AS fork_count_change_over_4_weeks,
        (fork_count::NUMERIC/nullif(prior_4_weeks_fork_count::NUMERIC, 0)) - 1 as fork_count_pct_change_over_4_weeks
    FROM 
        LaggedWeeklyCounts
    WHERE 
        prior_4_weeks_fork_count IS NOT NULL -- Only show rows where a 4-week prior value exists
)

SELECT 
    project_title, 
    (data_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS data_timestamp, 
    current_forks, 
    (prior_4_weeks_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS prior_4_weeks_timestamp,
    prior_4_weeks_fork_count, 
    fork_count_change_over_4_weeks, 
    fork_count_pct_change_over_4_weeks
FROM change_over_4_weeks
