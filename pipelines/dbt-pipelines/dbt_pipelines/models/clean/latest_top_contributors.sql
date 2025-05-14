-- clean/latest_top_contributors.sql
-- create a table that scores the latest contributors by activity
-- sources: latest_project_repos_contributors, latest_contributors
-- referenced models: latest_project_repos, latest_top_projects
-- use pre hooks to set work_mem and temp_buffers to 350MB and 175MB respectively
-- ref the latest_repo_exclude table to exclude non-crypto native, popular repos that make their way into the crypto ecosystem dataset

{{ config(
    materialized='table',
    unique_key='contributor_unique_id_builder_love || data_timestamp',
    tags=['latest_clean_data'],
    pre_hook="SET work_mem = '500MB'; SET temp_buffers = '200MB';",
    post_hook="RESET work_mem; RESET temp_buffers;"
) }}

with not_a_bot_account as (
  select
    lcd.contributor_node_id,
    lcd.contributor_unique_id_builder_love

  from {{ ref('latest_contributor_data') }} lcd left join {{ ref('latest_contributor_activity') }} lca
    on lcd.contributor_node_id = lca.contributor_node_id left join {{ source('clean_schema','latest_contributors_clean') }} lc
    on lcd.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love

  WHERE lca.has_contributed_in_last_year = true
  and lc.contributor_type <> 'bot'
  and LOWER(lcd.bio) like '%not a bot account%'
),

sure_bot as (
  select
    lcd.contributor_node_id,
    lcd.contributor_unique_id_builder_love

  from {{ ref('latest_contributor_data') }} lcd left join {{ ref('latest_contributor_activity') }} lca
    on lcd.contributor_node_id = lca.contributor_node_id left join {{ source('clean_schema','latest_contributors_clean') }} lc
    on lcd.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love

  WHERE lca.has_contributed_in_last_year = true
  and lc.contributor_type <> 'bot'
  and lcd.contributor_node_id not in(select DISTINCT contributor_node_id from not_a_bot_account)
  and (LOWER(lcd.bio) like '%i am a bot%' or LOWER(lcd.bio) like '%i''m a bot%' or LOWER(lcd.bio) like '%bot account%')
  or LOWER(lcd.bio) like '%i''m a helpful bot%' or LOWER(lcd.bio) like '%i''m a github bot%' or LOWER(lcd.bio) like '%i''m a friendly bot%'
  or LOWER(lcd.bio) like '%i am a github bot%' or LOWER(lcd.bio) like '%i am a helpful bot%' or LOWER(lcd.bio) like '%i am friendly bot%'
  or LOWER(lcd.bio) like '%i''m a friendly github bot%' or LOWER(lcd.bio) like '%i''m a helpful gitHub bot%' or LOWER(lcd.bio) like '%i''m a robot%'
  or LOWER(lcd.bio) like '%i am a robot%' or LOWER(lcd.bio) like '%robot account%'
),

sure_bot_login as (
  select
    lcd.contributor_node_id,
    lcd.contributor_unique_id_builder_love

  from {{ ref('latest_contributor_data') }} lcd left join {{ ref('latest_contributor_activity') }} lca
    on lcd.contributor_node_id = lca.contributor_node_id left join {{ source('clean_schema','latest_contributors_clean') }} lc
    on lcd.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love left join {{ ref('latest_contributor_following') }} lcf
    on lcd.contributor_node_id = lcf.contributor_node_id

  WHERE lca.has_contributed_in_last_year = true
  and lc.contributor_type <> 'bot'
  and lcd.contributor_node_id not in(select DISTINCT contributor_node_id from not_a_bot_account)
  and lcd.contributor_node_id not in(select DISTINCT contributor_node_id from sure_bot)
  and lcf.total_following_count = 0
  and LOWER(lc.contributor_login) like '%bot%'
),

manual_identify_bot as (
  select lcd.contributor_node_id,
    lcd.contributor_unique_id_builder_love

  from {{ ref('latest_contributor_data') }} lcd left join {{ source('clean_schema','latest_contributors_clean') }} lc
    on lcd.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love 
  where lc.contributor_login in ('bors')
),

bots as (
  select sure_bot.contributor_node_id,
    sure_bot.contributor_unique_id_builder_love

  from sure_bot
  union
  select sure_bot_login.contributor_node_id,
    sure_bot_login.contributor_unique_id_builder_love

  from sure_bot_login
  union 
  select manual_identify_bot.contributor_node_id,
    manual_identify_bot.contributor_unique_id_builder_love

  from manual_identify_bot
),

active_non_bot_contributors as (
  select 
    repo,
    lprc.contributor_unique_id_builder_love,
    contributor_login,
    contributor_contributions,
    lprc.data_timestamp

  from {{ source('clean_schema','latest_project_repos_contributors_clean') }} lprc inner join {{ source('clean_schema','latest_contributors_clean')}} lc
    on lprc.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love left join {{ ref('latest_contributor_data')}} lcd
    on lc.contributor_unique_id_builder_love = lcd.contributor_unique_id_builder_love left join {{ ref('latest_contributor_activity')}} lca
    on lcd.contributor_node_id = lca.contributor_node_id

  where lower(lc.contributor_type) <> 'bot' 
  and lcd.contributor_node_id not in (select contributor_node_id from bots) -- remove unidentified bots
  and (lcd.is_active = TRUE or lcd.is_active is NULL) -- drop false values, inidcating inactive
  and (lca.has_contributed_in_last_year = true or lca.has_contributed_in_last_year is NULL) -- drop false values, indicating inactive
),

repo_quality_weight_score as (
  select 
    cr.contributor_unique_id_builder_love,
    ltr.repo,
    ltr.weighted_score,
    cr.contributor_contributions,
    2 ^ (1 - ltr.weighted_score) as repo_quality_weight,
    cr.contributor_contributions * (2 ^ (1 - ltr.weighted_score)) as repo_quality_weighted_contribution_score

  from {{ref('latest_top_repos')}} ltr left join active_non_bot_contributors cr 
    on ltr.repo = cr.repo
  
  where ltr.repo not in (select distinct repo from {{ ref('latest_repo_exclude') }})
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

  from active_non_bot_contributors

  group by 1
),

-- get non-forked (og) repo metrics by contributor

contributors_og_repos as (
  select 
    c.repo,
    c.contributor_unique_id_builder_love,
    c.contributor_contributions

  from active_non_bot_contributors c inner join {{ ref('latest_project_repos_is_fork') }} f
    on c.repo = f.repo 

  where f.is_fork = FALSE 
  and c.repo not in (select distinct repo from {{ ref('latest_repo_exclude') }})
),

top_og_repo_contributors as (
  select 
    c.contributor_unique_id_builder_love,
    sum(c.contributor_contributions) contributions_to_og_repos,
    sum(c.contributor_contributions * (2 ^ (1 - COALESCE(ltr.weighted_score,0)))) total_og_repo_quality_weighted_contribution_score

  from contributors_og_repos c left join {{ref('latest_top_repos')}} ltr
    on c.repo = ltr.repo

  group by 1
),

top_contributor_metrics as (
  select 
    contributor_login,
    lc.contributor_type,
    lcdl.dominant_language,
    lcd.location,
    lc.contributor_unique_id_builder_love,
    coalesce(total_repos_contributed_to, 0) as total_repos_contributed_to,
    coalesce(total_contributions, 0) as total_contributions,
    coalesce(contributions_to_og_repos, 0) as contributions_to_og_repos,
    coalesce(total_repo_quality_weighted_contribution_score, 0) as total_repo_quality_weighted_contribution_score,
    coalesce(total_og_repo_quality_weighted_contribution_score, 0) as total_og_repo_quality_weighted_contribution_score,
    coalesce(lcfc.followers_total_count, 0) as followers_total_count,
    lc.contributor_html_url,
    lc.data_timestamp

from {{ source('clean_schema','latest_contributors_clean')}} lc left join sum_repo_quality_weight sr
  on lc.contributor_unique_id_builder_love = sr.contributor_unique_id_builder_love left join contributor_repo_activity cra
  on lc.contributor_unique_id_builder_love = cra.contributor_unique_id_builder_love left join {{ ref('latest_contributor_dominant_language') }} lcdl
  on lc.contributor_unique_id_builder_love = lcdl.contributor_unique_id_builder_love left join top_og_repo_contributors torc
  on lc.contributor_unique_id_builder_love = torc.contributor_unique_id_builder_love left join {{ ref('latest_contributor_data') }} lcd
  on lc.contributor_unique_id_builder_love = lcd.contributor_unique_id_builder_love left join {{ ref('latest_contributor_follower_count') }} lcfc
  on lcd.contributor_node_id = lcfc.contributor_node_id

-- filter out bots from the final contributor list
-- we use contributor_unique_id_builder_love since latest_contributors likely contains legacy contributor_node_id values
where lc.contributor_type <> 'bot'
and lc.contributor_unique_id_builder_love not in (select distinct contributor_unique_id_builder_love from bots)
),

normalized_top_contributors as (
  select 
    contributor_login,
    contributor_type,
    contributor_unique_id_builder_love,
    contributor_html_url,
    location,
    dominant_language,
    total_repos_contributed_to,
    total_contributions,
    contributions_to_og_repos,
    total_repo_quality_weighted_contribution_score,
    total_og_repo_quality_weighted_contribution_score,
    followers_total_count,
    (total_repos_contributed_to - MIN(total_repos_contributed_to) OVER ())::NUMERIC / NULLIF((MAX(total_repos_contributed_to) OVER () - MIN(total_repos_contributed_to) OVER ())::NUMERIC,0) AS normalized_total_repos_contributed_to,
    (total_contributions - MIN(total_contributions) OVER ())::NUMERIC / NULLIF((MAX(total_contributions) OVER () - MIN(total_contributions) OVER ())::NUMERIC,0) AS normalized_total_contributions,
    (total_repo_quality_weighted_contribution_score - MIN(total_repo_quality_weighted_contribution_score) OVER ())::NUMERIC / NULLIF((MAX(total_repo_quality_weighted_contribution_score) OVER () - MIN(total_repo_quality_weighted_contribution_score) OVER ())::NUMERIC,0) AS normalized_total_repo_quality_weighted_contribution_score,
    (total_og_repo_quality_weighted_contribution_score - MIN(total_og_repo_quality_weighted_contribution_score) OVER ())::NUMERIC / NULLIF((MAX(total_og_repo_quality_weighted_contribution_score) OVER () - MIN(total_og_repo_quality_weighted_contribution_score) OVER ())::NUMERIC,0) AS normalized_total_og_repo_quality_weighted_contribution_score,
    (followers_total_count - MIN(followers_total_count) OVER ())::NUMERIC / NULLIF((MAX(followers_total_count) OVER () - MIN(followers_total_count) OVER ())::NUMERIC,0) AS normalized_followers_total_count,
    data_timestamp

  from top_contributor_metrics
),

ranked_contributors as (
  select 
    contributor_login,
    contributor_type,
    dominant_language,
    location,
    contributor_unique_id_builder_love,
    contributor_html_url,
    total_repos_contributed_to,
    total_contributions,
    contributions_to_og_repos,
    total_repo_quality_weighted_contribution_score,
    total_og_repo_quality_weighted_contribution_score,
    followers_total_count,
    normalized_total_repos_contributed_to,
    normalized_total_contributions,
    normalized_total_repo_quality_weighted_contribution_score,
    normalized_total_og_repo_quality_weighted_contribution_score,
    normalized_followers_total_count,
    (
      (coalesce(normalized_total_repos_contributed_to,0) * .05) + 
      (coalesce(normalized_total_contributions,0) * .20) +
      (coalesce(normalized_total_repo_quality_weighted_contribution_score,0) * .40) +
      (coalesce(normalized_total_og_repo_quality_weighted_contribution_score,0) * .25) +
      (coalesce(normalized_followers_total_count,0) * .10)
    ) as weighted_score,
    data_timestamp
  
  from normalized_top_contributors

),

final_ranking as (
  select 
    contributor_login,
    contributor_type,
    dominant_language,
    location,
    contributor_unique_id_builder_love,
    contributor_html_url,
    total_repos_contributed_to::bigint,
    total_contributions::bigint,
    contributions_to_og_repos::bigint,
    total_repo_quality_weighted_contribution_score::numeric,
    total_og_repo_quality_weighted_contribution_score::numeric,
    followers_total_count::bigint,
    normalized_total_repos_contributed_to::numeric,
    normalized_total_contributions::numeric,
    normalized_total_repo_quality_weighted_contribution_score::numeric,
    normalized_total_og_repo_quality_weighted_contribution_score::numeric,
    normalized_followers_total_count::numeric,
    weighted_score::numeric,
    (weighted_score * 100) as weighted_score_index,
    RANK() OVER (
      ORDER BY normalized_total_repo_quality_weighted_contribution_score DESC
    ) as normalized_total_repo_quality_weighted_contribution_score_rank,
    RANK() OVER (
      ORDER BY weighted_score DESC
    ) AS contributor_rank,
    NTILE(4) OVER (
      ORDER BY weighted_score DESC
    ) AS quartile_bucket,
    data_timestamp

  from ranked_contributors
)

select * 
from final_ranking
order by weighted_score desc