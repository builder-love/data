-- models/clean/latest_contributors.sql
-- if clean.latest_contributors table exists, replace it with the raw.latest_contributors table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='contributor_unique_id_builder_love || data_timestamp',
    tags=['latest_clean_data']
) }}

-- the raw schema table has multiple data_timestamp values since we are only processing new data in raw.project_repos_contributors
-- for the clean schema table, we can use today's date as the data_timestamp
SELECT 
    contributor_unique_id_builder_love,
    contributor_login,
    contributor_id,
    contributor_node_id,
    contributor_avatar_url,
    contributor_gravatar_id,
    contributor_url,
    contributor_html_url,
    contributor_followers_url,
    contributor_following_url,
    contributor_gists_url,
    contributor_starred_url,
    contributor_subscriptions_url,
    contributor_organizations_url,
    contributor_repos_url,
    contributor_events_url,
    contributor_received_events_url,
    contributor_type,
    contributor_user_view_type,
    contributor_name,
    contributor_email,
    {{ dbt.current_timestamp() }}::timestamp as data_timestamp
FROM {{ source('raw', 'latest_contributors') }} 