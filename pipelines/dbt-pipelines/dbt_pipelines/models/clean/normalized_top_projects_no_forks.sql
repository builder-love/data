-- models/clean/normalized_top_projects_no_forks.sql
-- note, we drop commit and contributor history of forked repos; 
-- stargaze, watcher, and fork count of the forked repo is maintained as the metric history is not maintained in the fork
-- calculate top_project score
-- metrics and weights include: 
--   - total all-time commit count (.15), total all-time fork count (.15), total all-time stargaze count (.15), total all-time contributor count (.15), total all-time watcher count (.025), total all-time is_not_fork_ratio (.025)
--   - one month change commit count (.10), one month change fork count (.10), one month change stargaze count (.10), one month change contributor count (.10), one month change watcher count (.025), one month change is_not_fork_ratio (.025)

-- rm from project list:
-- General
-- Ethereum L2s
-- Ethereum Virtual Machine Stack
-- EVM Compatible L1 and L2
-- EVM Compatible Layer 1s
-- EVM Compatible Layer 1s, 2s, and Dapps
-- EVM Compatible Layer 2s
-- Solana Virtual Machine Stack
-- SVM Layer 1 and Layer 2s
-- SVM Layer 1s
-- Open Source Cryptography
-- ....

{{ config(
    materialized='table',
    unique_key='project_title || report_date',
    tags=['timestamp_normalized']
) }}

-- reference no-forks models
{{ generate_normalized_top_projects(
    commit_count_model='normalized_project_commit_count_no_forks',
    contributor_count_model='normalized_project_contributor_count_no_forks',
    commit_change_model='four_week_change_project_commit_count_no_forks',
    contributor_change_model='four_week_change_project_contributor_count_no_forks'
) }}