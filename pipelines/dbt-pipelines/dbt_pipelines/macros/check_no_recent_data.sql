-- macros/check_no_recent_data.sql
{% macro check_no_recent_data(model_name, column_name, days_ago) %}

WITH max_timestamp AS (
    SELECT
        MAX({{ column_name }}) as max_ts
    FROM {{ model_name }}
)
SELECT
    max_ts
FROM max_timestamp
WHERE max_ts < CURRENT_TIMESTAMP - INTERVAL '{{ days_ago }} days'

{% endmacro %}