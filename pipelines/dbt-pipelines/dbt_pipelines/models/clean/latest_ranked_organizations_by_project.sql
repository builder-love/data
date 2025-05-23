-- models/clean/latest_ranked_organizations_by_project.sql

{{ config(
    materialized='table',
    unique_key='project_title || project_organization_url || data_timestamp',
    tags=['latest_clean_data']
) }}

SELECT
    lpo.project_title,
    ltpo.project_organization_url,
    ltpo.data_timestamp,
    ltpo.org_rank,
    ltpo.org_rank_category,
    ltpo.weighted_score_index,
    ROW_NUMBER() OVER (PARTITION BY lpo.project_title ORDER BY ltpo.org_rank ASC) as rn
FROM
    {{ ref('latest_top_project_organizations') }} ltpo
INNER JOIN  
    {{ ref('latest_project_organizations') }} lpo ON ltpo.project_organization_url = lpo.project_organization_url