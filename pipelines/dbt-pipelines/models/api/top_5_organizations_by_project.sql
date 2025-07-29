-- models/api/top_5_organizations_by_project.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || project_organization_url || latest_data_timestamp',
        tags=['api_data']
    ) 
}} 

SELECT
    project_title,
    project_organization_url,
    TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp,
    org_rank,
    org_rank_category,
    weighted_score_index
FROM
    {{ source('prod_schema','latest_ranked_organizations_by_project_prod') }}
WHERE rn <= 5
and project_title is not null
and project_organization_url is not null

ORDER BY
    project_title ASC, 
    org_rank ASC