-- create a table that shows the latest contributor follower count

{{ config(
    materialized='table',
    unique_key='contributor_node_id || data_timestamp',
    tags=['latest_clean_data']
) }}


WITH latest_timestamp AS (
    SELECT MAX(data_timestamp) as max_ts
    FROM {{ ref('normalized_contributor_follower_count') }} 
)
SELECT
    contributor_node_id,
    followers_total_count,
    data_timestamp
FROM {{ ref('normalized_contributor_follower_count') }} 
WHERE data_timestamp = (SELECT max_ts FROM latest_timestamp)