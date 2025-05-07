-- clean/latest_top_contributors.sql
-- create a table that scores the latest contributors by activity
-- sources: latest_project_repos_contributors, latest_contributors
-- referenced models: latest_project_repos, latest_top_projects

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

  from {{ source('clean','latest_project_repos_contributors') }} lprc inner join {{ source('clean','latest_contributors')}} lc
    on lprc.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love

  where lower(contributor_type) <> 'bot' and contributor_unique_id_builder_love not in('|')
),

repo_quality_weight_score as (
  select 
    cr.contributor_unique_id_builder_love,
    ltr.repo,
    ltr.weighted_score,
    cr.contributor_contributions,
    2 ^ (1 - ltr.weighted_score) as repo_quality_weight,
    cr.contributor_contributions * (2 ^ (1 - ltr.weighted_score)) as repo_quality_weighted_contribution_score

  from {{ref('latest_top_repos')}} ltr left join non_bot_contributors cr 
    on ltr.repo = cr.repo
),

sum_repo_quality_weight as (
  select 
    contributor_unique_id_builder_love,
    sum(repo_quality_weighted_contribution_score) total_repo_quality_weighted_contribution_score
  from repo_quality_weight_score
  group by 1
),

contributor_repo_activity as (
  select 
    contributor_unique_id_builder_love,
    count(distinct repo) total_repos_contributed_to,
    sum(contributor_contributions) total_contributions

  from non_bot_contributors

  group by 1
)

select 
  contributor_login,
  lc.contributor_unique_id_builder_love,
  total_repos_contributed_to,
  total_contributions,
  total_repo_quality_weighted_contribution_score,
  lc.data_timestamp

from {{ source('clean','latest_contributors')}} lc left join sum_repo_quality_weight sr
  on lc.contributor_unique_id_builder_love = sr.contributor_unique_id_builder_love left join contributor_repo_activity cra
  on lc.contributor_unique_id_builder_love = cra.contributor_unique_id_builder_love