-- models/clean/latest_project_contributor_count.sql
-- if clean.latest_project_contributor_count table exists, replace it with the latest data from clean.latest_project_repos_contributors table
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
    repo,
    contributor_login,
    data_timestamp
  from {{ source('clean', 'latest_project_repos_contributors') }}
  where LOWER(contributor_type) IN ('user', 'anonymous') 
    and contributor_contributions > 0 
    and contributor_contributions is not null
),

project_contributor_count as (
  select lpr.project_title, 
  count(distinct contributors.contributor_login) contributor_count,
  max(contributors.data_timestamp) data_timestamp

  from latest_project_repos_contributors contributors left join {{ source('clean', 'latest_project_repos') }} lpr
    on contributors.repo = lpr.repo
  where lpr.project_title is not null
  group by 1
)

select project_title,
contributor_count,
data_timestamp

from project_contributor_count

order by 2 DESC