-- models/clean/four_week_change_project_repos_contributor_count.sql
-- the periodicity of the data is monthly so we look back one period to get the 4 week change

{{ 
    config(
        materialized='view',
        unique_key='repo || data_timestamp',
        tags=['period_change_data']
    ) 
}} 

WITH LaggedWeeklyCounts AS (
    -- Step 3: Use LAG(..., 4) to get the value from 4 weeks prior
    SELECT
        repo,
        data_timestamp,
        contributor_count,
        LAG(contributor_count, 1) OVER (
            PARTITION BY repo 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_contributor_count,
        LAG(data_timestamp, 1) OVER (
            PARTITION BY repo 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_timestamp -- The timestamp for the prior count
    FROM 
        {{ ref('normalized_project_repos_contributor_count') }}

    where contributor_count IS NOT NULL 
),

-- Step 4: Final Calculation
change_over_4_weeks as (
    SELECT
        repo,
        data_timestamp,
        contributor_count AS current_contributors,
        prior_4_weeks_timestamp, 
        prior_4_weeks_contributor_count,
        (contributor_count - prior_4_weeks_contributor_count) AS contributor_count_change_over_4_weeks,
        (contributor_count::NUMERIC/nullif(prior_4_weeks_contributor_count::NUMERIC, 0)) - 1 as contributor_count_pct_change_over_4_weeks
    FROM 
        LaggedWeeklyCounts
    WHERE 
        prior_4_weeks_contributor_count IS NOT NULL -- Only show rows where a 4-week prior value exists
)

SELECT 
    repo, 
    (data_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS data_timestamp, 
    current_contributors::BIGINT, 
    (prior_4_weeks_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS prior_4_weeks_timestamp,
    prior_4_weeks_contributor_count::BIGINT, 
    contributor_count_change_over_4_weeks::INTEGER, 
    contributor_count_pct_change_over_4_weeks::NUMERIC
FROM change_over_4_weeks