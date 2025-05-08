{% test check_row_count_stability(model, lower_threshold_factor=0.5, upper_threshold_factor=1.5, min_rows_for_check=10) %}
  {#
    This test checks if the row count of the current model build is within a certain percentage range
    of the row count of the existing version of the table in the database.
    It fails if the new row count is outside:
    [existing_count * lower_threshold_factor, existing_count * upper_threshold_factor]
    The check is only applied if the existing table has at least `min_rows_for_check`.
  #}

  {% set current_model_relation = model %} {# `model` is the relation object for the model being tested (newly built version) #}

  {# Construct a relation object for the existing table in the target schema #}
  {% set existing_relation = adapter.get_relation(
                                database=model.database,
                                schema=model.schema,
                                identifier=model.name
                              ) %}

  WITH new_data_count AS (
      SELECT COUNT(*) as n_count FROM {{ current_model_relation }}
  )
  {% if existing_relation and not flags.FULL_REFRESH %} {# Only compare if existing relation exists and its not a full refresh run overriding it #}
  , existing_data_count AS (
      SELECT COUNT(*) as e_count FROM {{ existing_relation }} {# This queries the table as it currently exists in the DB #}
  )
  SELECT
      ndc.n_count AS new_row_count,
      edc.e_count AS existing_row_count,
      (edc.e_count * {{ lower_threshold_factor }}) AS lower_bound,
      (edc.e_count * {{ upper_threshold_factor }}) AS upper_bound
  FROM new_data_count ndc, existing_data_count edc
  WHERE
      edc.e_count >= {{ min_rows_for_check }} AND -- Only apply check if existing table had enough rows
      NOT (
          ndc.n_count >= (edc.e_count * {{ lower_threshold_factor }}) AND
          ndc.n_count <= (edc.e_count * {{ upper_threshold_factor }})
      )
  {% else %}
      -- If existing table doesn't exist (e.g., first run, or after a full refresh was just done),
      -- or it's a full refresh, this test effectively passes by returning no rows.
      SELECT 0 AS new_row_count, 0 AS existing_row_count, 0 AS lower_bound, 0 AS upper_bound
      WHERE FALSE -- Ensures no rows are returned, so test passes
  {% endif %}
{% endtest %}