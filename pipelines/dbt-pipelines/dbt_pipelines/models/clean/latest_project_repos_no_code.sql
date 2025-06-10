-- models/clean/latest_project_repos_no_code.sql
-- if clean.latest_project_repos_no_code table exists, replace it
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT
  repo,
  data_timestamp
FROM
  {{ ref('latest_project_repos_languages') }}
GROUP BY
  repo, data_timestamp
HAVING
  (COUNT(DISTINCT language_name) = 1 AND MAX(language_name) = 'Markdown')
  or COUNT(DISTINCT language_name) = 0