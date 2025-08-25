-- models/clean/latest_contributors.sql
-- if clean.latest_contributors table exists, replace it with the raw.latest_contributors table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='contributor_unique_id_builder_love || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT *
FROM {{ source('raw', 'latest_contributors') }} 