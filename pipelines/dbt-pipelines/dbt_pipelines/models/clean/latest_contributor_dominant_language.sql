
-- models/clean/latest_contributor_dominant_language.sql
-- if clean.latest_contributor_dominant_language table exists, replace it with the raw.latest_contributor_dominant_language table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='contributor_unique_id_builder_love || data_timestamp',
    tags=['latest_clean_data']
) }}


with non_bot_contributors as (
  select 
    repo,
    lprc.contributor_unique_id_builder_love,
    contributor_login,
    contributor_contributions,
    lprc.data_timestamp

  from {{ source('clean','latest_project_repos_contributors') }} lprc inner join {{ source('clean','latest_contributors') }} lc
    on lprc.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love

  where lower(lc.contributor_type) <> 'bot' and lc.contributor_unique_id_builder_love not in('|')
),

contributor_repo_language as (
  select 
    contributor_unique_id_builder_love,
    dominant_language,
    count(distinct c.repo) as repo_count,
    max(c.data_timestamp) as data_timestamp -- use clean.latest_project_repos_contributors assuming repo/contributor list changes more frequently than languages

  from non_bot_contributors c left join {{ ref('latest_project_repos_dominant_language') }} l
    on c.repo = l.repo

  group by 1, 2
),

contributor_repo_language_top as (
  select distinct on (contributor_unique_id_builder_love)
    contributor_unique_id_builder_love,
    dominant_language,
    repo_count,
    data_timestamp

  from contributor_repo_language

  order by contributor_unique_id_builder_love, repo_count desc, dominant_language asc
)

select
  contributor_unique_id_builder_love,
  dominant_language,
  repo_count,
  data_timestamp

from contributor_repo_language_top
