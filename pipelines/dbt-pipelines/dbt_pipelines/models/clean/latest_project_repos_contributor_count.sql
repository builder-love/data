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
  count(distinct lprc.contributor_unique_id_builder_love) as contributor_count,
  max(lprc.data_timestamp) data_timestamp
from {{ source('clean_schema', 'latest_project_repos_contributors_clean') }} lprc left join {{ source('clean_schema', 'latest_contributors_clean') }} lc 
  on lprc.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love
where LOWER(lc.contributor_type) IN ('user', 'anonymous') 
    and lprc.contributor_contributions > 0 
    and lprc.contributor_contributions is not null
group by 1
order by 2 desc