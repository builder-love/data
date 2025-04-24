-- models/clean/latest_project_sub_ecosystems.sql
{{ config(
    materialized='table',
    unique_key='project_title || sub_ecosystems || data_timestamp',
    tags=['latest_clean_data']
) }}

with projects as (
  select 
    project_title, 
    sub_ecosystems

  from {{ source('raw', 'crypto_ecosystems_raw_file') }}
  where sub_ecosystems <> '{}'
),

distinct_project_ecosystems as (
  select distinct
    project_title, 
    sub_ecosystems,
    NOW() AT TIME ZONE 'utc' as data_timestamp

  from projects
)

select * from distinct_project_ecosystems