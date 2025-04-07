-- models/clean/latest_project_repos_contributor_count.sql
-- if clean.latest_project_repos_contributor_count table exists, replace it with the latest data from clean.latest_project_repos_contributors table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='project_title',
    tags=['latest_clean_data']
) }}

-- countributor count by repo
-- contributors are either a 'user' or 'anonymous'
-- contributors made at least one contribution
select 
  repo, 
  count(distinct contributor_login) as contributor_count,
  max(data_timestamp) data_timestamp
from {{ source('clean','latest_project_repos_contributors') }} 
where LOWER(contributor_type) IN ('user', 'anonymous') 
    and contributor_contributions > 0 
    and contributor_contributions is not null
group by 1
order by 2 desc