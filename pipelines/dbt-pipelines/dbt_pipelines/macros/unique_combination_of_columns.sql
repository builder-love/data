-- macros/unique_combination_of_columns.sql

{% test unique_combination_of_columns(model, column_names) %}

SELECT
    {{ column_names | join(', ') }}
FROM
    {{ model }}
GROUP BY
    {{ column_names | join(', ') }}
HAVING
    COUNT(*) > 1

{% endtest %}