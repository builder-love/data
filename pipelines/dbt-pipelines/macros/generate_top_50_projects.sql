-- macros/generate_top_50_projects.sql

{% macro generate_top_50_projects(
    top_50_projects_model
) %}

  select 
    project_title,
    weighted_score,
    TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_data_timestamp
  from {{ source('prod_schema', top_50_projects_model) }}
  order by weighted_score desc 
  limit 50


{% endmacro %}