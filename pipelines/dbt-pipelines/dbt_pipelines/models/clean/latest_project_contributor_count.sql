-- models/clean/latest_project_contributor_count.sql
-- if clean.latest_project_contributor_count table exists, replace it with the latest data from clean.latest_contributors and clean.latest_project_repos_contributors tables
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='project_title',
    tags=['latest_clean_data']
) }}

-- countributor count by project
-- contributors are either a 'user' or 'anonymous'
-- contributors made at least one contribution
with latest_project_repos_contributors as (
  select 
    lprc.repo,
    lc.contributor_login,
    lprc.data_timestamp
  from {{ source('clean_schema', 'latest_project_repos_contributors_clean') }} lprc left join {{ source('clean_schema', 'latest_contributors_clean') }} lc 
    on lprc.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love
  where LOWER(lc.contributor_type) IN ('user', 'anonymous') 
    and lprc.contributor_contributions > 0 
    and lprc.contributor_contributions is not null
),

project_contributor_count as (
  select lpr.project_title, 
  count(distinct contributors.contributor_login) contributor_count,
  max(contributors.data_timestamp) data_timestamp

  from latest_project_repos_contributors contributors left join {{ source('clean_schema', 'latest_project_repos_clean') }} lpr
    on contributors.repo = lpr.repo
  where lpr.project_title is not null
  group by 1
)

select project_title,
contributor_count,
data_timestamp

from project_contributor_count

order by 2 DESC