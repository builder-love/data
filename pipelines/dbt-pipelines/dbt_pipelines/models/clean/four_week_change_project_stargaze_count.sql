-- models/clean/four_week_change_project_stargaze_count.sql
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
        stargaze_count,
        LAG(stargaze_count, 4) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_stargaze_count,
        LAG(data_timestamp, 4) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_timestamp -- The timestamp for the prior count
    FROM 
        {{ ref('normalized_project_stargaze_count') }}
    where stargaze_count IS NOT NULL
),

change_over_4_weeks as (
    SELECT
        project_title,
        data_timestamp,
        stargaze_count AS current_stargazes,
        prior_4_weeks_timestamp, 
        prior_4_weeks_stargaze_count,
        (stargaze_count - prior_4_weeks_stargaze_count) AS stargaze_count_change_over_4_weeks,
        (stargaze_count::NUMERIC/nullif(prior_4_weeks_stargaze_count::NUMERIC, 0)) - 1 as stargaze_count_pct_change_over_4_weeks
    FROM 
        LaggedWeeklyCounts
    WHERE 
        prior_4_weeks_stargaze_count IS NOT NULL -- Only show rows where a 4-week prior value exists
)

SELECT 
    project_title, 
    (data_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS data_timestamp, 
    current_stargazes, 
    (prior_4_weeks_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS prior_4_weeks_timestamp,
    prior_4_weeks_stargaze_count, 
    stargaze_count_change_over_4_weeks, 
    stargaze_count_pct_change_over_4_weeks
FROM change_over_4_weeks
