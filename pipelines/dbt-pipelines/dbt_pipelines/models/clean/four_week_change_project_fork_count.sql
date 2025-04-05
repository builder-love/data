-- models/clean/four_week_change_project_fork_count.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title',
        tags=['period_change_data']
    ) 
}} 

WITH WeeklySnapshot AS (
    -- Step 1: Find the latest timestamp for each project within each week
    SELECT
        project_title,
        DATE_TRUNC('week', data_timestamp)::DATE AS snapshot_week_start, -- Start date of the week
        MAX(data_timestamp) AS max_timestamp_in_week
    FROM 
        {{ source('clean', 'normalized_project_fork_count') }}
    WHERE 
        fork_count IS NOT NULL
    GROUP BY 
        1, 2
),

LatestWeeklyProjectForkCount AS (
    -- Step 2: Get the fork count associated with that latest timestamp per week
    SELECT
        ws.snapshot_week_start,
        ws.project_title,
        f.fork_count,
        ws.max_timestamp_in_week -- Keep the actual timestamp for context if needed
    FROM 
        WeeklySnapshot ws
    JOIN 
        {{ source('clean', 'normalized_project_fork_count') }} f 
        ON ws.project_title = f.project_title 
        AND ws.max_timestamp_in_week = f.data_timestamp
    WHERE 
        f.fork_count IS NOT NULL
),

LaggedWeeklyCounts AS (
    -- Step 3: Use LAG(..., 4) to get the value from 4 weeks prior
    SELECT
        snapshot_week_start,
        project_title,
        fork_count,
        max_timestamp_in_week, -- The timestamp for the current 'fork_count'
        LAG(fork_count, 4) OVER (
            PARTITION BY project_title 
            ORDER BY snapshot_week_start
        ) AS prior_4_weeks_fork_count,
        LAG(max_timestamp_in_week, 4) OVER (
            PARTITION BY project_title 
            ORDER BY snapshot_week_start
        ) AS prior_4_weeks_timestamp -- The timestamp for the prior count
    FROM 
        LatestWeeklyProjectForkCount
),

-- Step 4: Final Calculation
change_over_4_weeks as (
    SELECT
        project_title,
        max_timestamp_in_week AS data_timestamp,
        fork_count AS current_forks,
        prior_4_weeks_timestamp, 
        prior_4_weeks_fork_count,
        (fork_count - prior_4_weeks_fork_count) AS fork_count_change_over_4_weeks,
        (fork_count::NUMERIC/nullif(prior_4_weeks_fork_count::NUMERIC, 0)) - 1 as fork_count_pct_change_over_4_weeks
    FROM 
        LaggedWeeklyCounts
    WHERE 
        prior_4_weeks_fork_count IS NOT NULL -- Only show rows where a 4-week prior value exists
),

ranked_changes AS (
    -- Add a row number partitioned by project, ordered by timestamp descending
    SELECT
        c.*, -- Select all columns from the previous CTE
        ROW_NUMBER() OVER (PARTITION BY project_title ORDER BY data_timestamp DESC) as rn
    FROM change_over_4_weeks c
)
-- Final Step: Select only the latest row (rn=1) for each project
SELECT 
    project_title, 
    (data_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS data_timestamp, 
    current_forks, 
    (prior_4_weeks_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS prior_4_weeks_timestamp,
    prior_4_weeks_fork_count, 
    fork_count_change_over_4_weeks, 
    fork_count_pct_change_over_4_weeks
FROM ranked_changes
WHERE rn = 1
