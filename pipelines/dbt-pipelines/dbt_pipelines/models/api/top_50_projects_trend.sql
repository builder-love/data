-- models/api/top_50_projects_trend.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || report_date',
        tags=['api_data']
    ) 
}} 

with sorted_base as (
    select 
        project_title,
        report_date,
        data_timestamp,
        repo_count,
        weighted_score_index,
        fork_count,
        stargaze_count,
        commit_count,
        contributor_count,
        watcher_count,
        is_not_fork_ratio,
        commit_count_pct_change_over_4_weeks,
        contributor_count_pct_change_over_4_weeks,
        fork_count_pct_change_over_4_weeks,
        stargaze_count_pct_change_over_4_weeks,
        watcher_count_pct_change_over_4_weeks,
        is_not_fork_ratio_pct_change_over_4_weeks,
        project_rank,
        quartile_bucket,
        project_rank_category

    from {{ source('prod_schema', 'normalized_top_projects_prod') }}
    where project_title in(select distinct project_title from {{ ref('top_50_projects') }} )
    order by report_date desc, weighted_score_index desc
)

select 
    project_title,
    TO_CHAR(report_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS report_date,
    TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS data_timestamp,
    repo_count,
    weighted_score_index,
    fork_count,
    stargaze_count,
    commit_count,
    contributor_count,
    watcher_count,
    is_not_fork_ratio,
    commit_count_pct_change_over_4_weeks,
    contributor_count_pct_change_over_4_weeks,
    fork_count_pct_change_over_4_weeks,
    stargaze_count_pct_change_over_4_weeks,
    watcher_count_pct_change_over_4_weeks,
    is_not_fork_ratio_pct_change_over_4_weeks,
    project_rank,
    quartile_bucket,
    project_rank_category
from sorted_base