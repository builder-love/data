-- models/clean/latest_project_repos_dominant_language.sql
-- if clean.latest_project_repos_dominant_language table exists, replace it with the raw.latest_project_repos_dominant_language table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='repo || data_timestamp',
    tags=['latest_clean_data']
) }}

with repo_dominant_language as (
  SELECT DISTINCT ON (repo)
    repo,
    language_name AS dominant_language,
    size AS dominant_language_bytes,
    repo_languages_total_bytes,
    (size::numeric / NULLIF(repo_languages_total_bytes, 0)::numeric) AS dominant_language_percentage,
    data_timestamp
  FROM
      {{ ref('latest_project_repos_languages') }}
  WHERE language_name is not null 
  and size is not null 
  and repo_languages_total_bytes is not null
  ORDER BY
      repo, size DESC, language_name ASC
)

select 
  repo,
  dominant_language,
  dominant_language_bytes,
  dominant_language_percentage,
  data_timestamp

from repo_dominant_language