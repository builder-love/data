-- models/clean/latest_contributor_activity.sql
-- if clean.latest_contributor_activity table exists, replace it with the raw.latest_contributor_activity table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='contributor_node_id || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT *
FROM {{ source('raw', 'latest_contributor_activity') }} 