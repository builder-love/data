-- tests/crypto_ecosystems_is_unique.sql
-- this is a standalone test that is run by dagster
-- the test asserts that the combined columns project_title and repo and sub_ecosystem are unique; if any results are returned, the test will fail

SELECT 
    project_title,
    repo,
    sub_ecosystems,
    COUNT(*) as count
FROM {{ source('raw', 'crypto_ecosystems_raw_file_staging') }}
GROUP BY project_title, repo, sub_ecosystems
HAVING COUNT(*) > 1