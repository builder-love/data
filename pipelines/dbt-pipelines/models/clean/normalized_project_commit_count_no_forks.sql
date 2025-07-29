-- normalized_project_commit_count_no_forks.sql
-- create an incremental table in clean with data_timestamp intervals >6 days apart
-- this is a copy of the normalized_project_commit_count.sql model, but with the forks excluded

{{ config(
    materialized='incremental',
    unique_key='project_title || data_timestamp',
    tags=['timestamp_normalized']
) }}

-- depends_on: {{ ref('normalized_project_repos_commit_count') }}
-- depends_on: {{ ref('latest_project_repos_commit_count') }}
-- depends_on: {{ ref('latest_project_repos') }}
-- depends_on: {{ ref('latest_project_repos_is_fork') }}


{% if is_incremental() %}

    -- get stats for outlier exclusion and max data_timestamp to restrict intermmittent runs to n days apart
    {% set stats_query %}

    -- Calculate the count of records for each data_timestamp in the clean table
    with record_counts AS (

        SELECT
        data_timestamp,
        COUNT(*) AS record_count

        FROM {{ this }} -- Use {{ this }} to refer to the current model (clean table)
        GROUP BY data_timestamp
    )

    -- Calculate the mean of the record counts from the latest table, and the max data_timestamp
    SELECT
    MAX(data_timestamp) AS max_data_timestamp,
    AVG(record_count) AS mean_count
    FROM record_counts

    {% endset %}

    {% set stats_results = run_query(stats_query) %}

    {% if execute %}

        {% set max_data_timestamp = stats_results.columns[0].values()[0] %}
        {% set mean_count = stats_results.columns[1].values()[0] %}

    {% else %}

        {% set max_data_timestamp = (modules.datetime.datetime.fromisoformat(initial_load_timestamp.replace('Z', '+00:00'))).isoformat() %}
        {% set mean_count = 0 %}

    {% endif %}

    -- on incremental runs, add the latest commit count for each project
    WITH raw_data as (
    SELECT
        o.project_title,
        f.data_timestamp,
        CAST(SUM(f.commit_count) AS BIGINT) AS commit_count

    FROM
        {{ ref('latest_project_repos_commit_count') }} AS f
    LEFT JOIN
        {{ ref('latest_project_repos') }} AS o ON f.repo = o.repo
    LEFT JOIN
        {{ ref('latest_project_repos_is_fork') }} AS fork ON f.repo = fork.repo

    WHERE
        fork.is_fork <> TRUE
        AND o.project_title IS NOT NULL

    GROUP BY
        1, 2
    ),

    raw_data_add_row_count as (
        SELECT
        project_title,
        data_timestamp,
        commit_count,
        COUNT(*) OVER () AS total_row_count

        FROM raw_data
    )

    -- only keep results if the total row count is within 50% and 150% of the mean and latest data_timestamp is 6 days or more from the max data_timestamp
    SELECT
        project_title,
        data_timestamp,
        commit_count

    FROM
        raw_data_add_row_count

    where
        data_timestamp >= '{{ max_data_timestamp }}'::timestamp + INTERVAL '6 days'
        AND total_row_count <= ({{ mean_count }} * 1.5)
        AND total_row_count >= ({{ mean_count }} * 0.5)

{% else %}

    -- CTAS using all records from history, casting the current project <> repo relationship
    SELECT
        o.project_title,
        f.data_timestamp,
        CAST(SUM(f.commit_count) AS BIGINT) AS commit_count

    FROM
        {{ ref('normalized_project_repos_commit_count') }} AS f

    LEFT JOIN
        {{ ref('latest_project_repos') }} AS o ON f.repo = o.repo

    LEFT JOIN
        {{ ref('latest_project_repos_is_fork') }} AS fork ON f.repo = fork.repo

    WHERE
        fork.is_fork <> TRUE 
        AND o.project_title IS NOT NULL

    GROUP BY
        1, 2

{% endif %}