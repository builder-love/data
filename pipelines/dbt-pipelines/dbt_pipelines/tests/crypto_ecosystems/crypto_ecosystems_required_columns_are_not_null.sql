-- tests/crypto_ecosystems_required_columns_are_not_null.sql
-- this is a standalone test that is run by dagster
-- the test asserts that the required columns are not null; if any results are returned, the test will fail

SELECT *
FROM {{ source('raw', 'crypto_ecosystems_raw_file_staging') }}
WHERE project_title IS NULL
   OR repo IS NULL
   OR data_timestamp IS NULL
   OR id IS NULL