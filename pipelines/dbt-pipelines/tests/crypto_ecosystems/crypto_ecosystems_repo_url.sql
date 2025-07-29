-- tests/crypto_ecosystems_data_types.sql
-- this is a standalone test that is run by dagster
-- the test asserts that the repo column is a valid URL; if any results are returned, the test will fail

SELECT *
FROM {{ source('raw', 'crypto_ecosystems_raw_file_staging') }}
WHERE repo IS NOT NULL -- Only check non-null repos
  AND repo NOT LIKE 'http://%'
  AND repo NOT LIKE 'https://%'