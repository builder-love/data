-- models/clean/four_week_change_project_is_fork.sql
-- this table updates weekly, so we take a shortcut and lag 3 records to get the 4 week change

{{ 
    config(
        materialized='view',
        unique_key='project_title || data_timestamp',
        tags=['period_change_data']
    ) 
}} 

WITH LaggedWeeklyCounts AS (
    -- Use LAG(..., 3) to get the value from 4 weeks prior
    SELECT
        project_title,
        data_timestamp,
        is_not_fork_ratio,
        LAG(is_not_fork_ratio, 3) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_is_not_fork_ratio,
        LAG(data_timestamp, 3) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_timestamp -- The timestamp for the prior count
    FROM 
        {{ ref('normalized_project_is_fork') }}
    where is_not_fork_ratio IS NOT NULL
),

change_over_4_weeks as (
    SELECT
        project_title,
        data_timestamp,
        is_not_fork_ratio AS current_is_not_fork_ratio,
        prior_4_weeks_timestamp, 
        prior_4_weeks_is_not_fork_ratio,
        (is_not_fork_ratio - prior_4_weeks_is_not_fork_ratio) AS is_not_fork_ratio_change_over_4_weeks,
        (is_not_fork_ratio::NUMERIC/nullif(prior_4_weeks_is_not_fork_ratio::NUMERIC, 0)) - 1 as is_not_fork_ratio_pct_change_over_4_weeks
    FROM 
        LaggedWeeklyCounts
    WHERE 
        prior_4_weeks_is_not_fork_ratio IS NOT NULL -- Only show rows where a 4-week prior value exists
)

SELECT 
    project_title, 
    (data_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS data_timestamp, 
    current_is_not_fork_ratio, 
    (prior_4_weeks_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS prior_4_weeks_timestamp,
    prior_4_weeks_is_not_fork_ratio, 
    is_not_fork_ratio_change_over_4_weeks, 
    is_not_fork_ratio_pct_change_over_4_weeks
FROM change_over_4_weeks
