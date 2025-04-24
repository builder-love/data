-- tests/crypto_ecosystems_row_count.sql
-- this is a standalone test that is run by dagster
-- the test asserts that the count of rows in staging is not greater than +- 50% of the row count in crypto_ecosystems_raw_file; if any results are returned, the test will fail

with staging_row_count as (
    SELECT COUNT(*) as count
    FROM {{ source('raw', 'crypto_ecosystems_raw_file_staging') }}
),
prod_row_count as (
    SELECT COUNT(*) as count
    FROM {{ source('raw', 'crypto_ecosystems_raw_file') }}
)

SELECT 
    staging_row_count.count,
    prod_row_count.count
FROM staging_row_count
CROSS JOIN prod_row_count
WHERE ABS(staging_row_count.count - prod_row_count.count) > 0.5 * LEAST(staging_row_count.count, prod_row_count.count)