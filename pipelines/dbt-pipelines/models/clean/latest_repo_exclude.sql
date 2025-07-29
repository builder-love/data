-- create a table that lists the repos that are very popular and are not crypto native
-- these repos make their way into the crypto ecosystems dataset sometimes and introduce outliers
-- we want to exclude them from our analysis where appropriate

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}

WITH repo_exclude_table AS (
    SELECT
        CAST(NULL AS VARCHAR) AS repo,
        CAST(NULL AS TIMESTAMP) AS data_timestamp
    WHERE 1=0 -- This ensures the CTE structure is defined but no rows are selected from this part

    UNION ALL

    SELECT
        'https://github.com/fogtech-io/linux' AS repo,
        TIMESTAMP '2023-03-15 00:00:00' AS data_timestamp
)

SELECT
    repo,
    data_timestamp
FROM repo_exclude_table