-- macros/generate_top_projects_view.sql

{% macro generate_top_projects_view(
    top_projects_view_model
) %}

select 
    project_title,
    TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp,
    contributor_count,
    contributor_count_pct_change_over_4_weeks,
    repo_count,
    fork_count,
    fork_count_pct_change_over_4_weeks,
    stargaze_count,
    stargaze_count_pct_change_over_4_weeks,
    commit_count,
    commit_count_pct_change_over_4_weeks,
    watcher_count,
    watcher_count_pct_change_over_4_weeks,
    is_not_fork_ratio,
    is_not_fork_ratio_pct_change_over_4_weeks,
    project_rank,
    prior_4_weeks_project_rank,
    absolute_project_rank_change_over_4_weeks,
    rank_of_project_rank_change_over_4_weeks,
    quartile_bucket,
    project_rank_category,
    weighted_score_index,
    weighted_score_sma,
    prior_4_weeks_weighted_score

from {{ source('prod_schema', top_projects_view_model) }}
order by weighted_score_sma desc 


{% endmacro %}