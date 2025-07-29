-- top_1000_contributor_count_by_language.sql

{{ 
    config(
        materialized='view',
        unique_key='dominant_language || latest_data_timestamp',
        tags=['api_data']
    ) 
}} 

select 
  dominant_language,
  developer_count,
  TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp

from {{ source('prod_schema', 'normalized_contributor_dominant_language_top_1000_prod') }}
order by data_timestamp desc, developer_count desc