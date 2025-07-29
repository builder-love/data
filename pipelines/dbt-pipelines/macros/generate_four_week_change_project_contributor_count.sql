-- macros/generate_four_week_change_project_contributor_count.sql

{% macro generate_four_week_change_project_contributor_count(
    contributor_count_model
) %}


WITH LaggedWeeklyCounts AS (
    -- Step 3: Use LAG(..., 4) to get the value from 4 weeks prior
    SELECT
        project_title,
        data_timestamp,
        contributor_count,
        LAG(contributor_count, 1) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_contributor_count,
        LAG(data_timestamp, 1) OVER (
            PARTITION BY project_title 
            ORDER BY data_timestamp
        ) AS prior_4_weeks_timestamp -- The timestamp for the prior count
    FROM 
        {{ ref(contributor_count_model) }}

    where contributor_count IS NOT NULL 
),

-- Step 4: Final Calculation
change_over_4_weeks as (
    SELECT
        project_title,
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
    project_title, 
    (data_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS data_timestamp, 
    current_contributors, 
    (prior_4_weeks_timestamp AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS prior_4_weeks_timestamp,
    prior_4_weeks_contributor_count, 
    contributor_count_change_over_4_weeks, 
    contributor_count_pct_change_over_4_weeks
FROM change_over_4_weeks

{% endmacro %}