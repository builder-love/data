-- models/api/top_100_contributors.sql

{{ 
    config(
        materialized='view',
        unique_key='contributor_unique_id_builder_love || latest_data_timestamp',
        tags=['api_data']
    ) 
}} 

select 
    contributor_unique_id_builder_love,
    contributor_login,
    contributor_type,
    dominant_language,
    contributor_html_url,
    total_repos_contributed_to,
    total_contributions,
    contributions_to_og_repos,
    normalized_total_repo_quality_weighted_contribution_score_rank,
    weighted_score_index,
    quartile_bucket,
    contributor_rank,
    TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp

from {{ source('prod', 'latest_top_contributors') }}
order by weighted_score desc
limit 100