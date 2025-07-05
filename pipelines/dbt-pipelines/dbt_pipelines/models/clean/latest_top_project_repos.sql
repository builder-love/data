-- models/clean/latest_top_project_repos.sql
-- convenience table for joining top_repos with project_title
{{ config(
    materialized='table',
    unique_key='project_title || repo || report_date',
    tags=['latest_clean_data']
) }}

select 
  lpr.project_title,
  ltr.data_timestamp,
  ltr.repo,
  ltr.fork_count,
  ltr.stargaze_count,
  ltr.watcher_count,
  ltr.weighted_score_index,
  ltr.repo_rank,
  ltr.quartile_bucket,
  ltr.repo_rank_category,
  lprf.predicted_is_dev_tooling,
  lprf.predicted_is_educational,
  lprf.predicted_is_scaffold,
  lprf.predicted_is_app,
  lprf.predicted_is_infrastructure

from {{ ref('latest_top_repos') }} ltr left join {{ ref('latest_project_repos') }} lpr
  on ltr.repo = lpr.repo left join {{ ref('latest_project_repos_features') }} lprf
  on ltr.repo = lprf.repo

where LOWER(lpr.project_title) NOT IN (
    'general',
    'ethereum l2s',
    'ethereum virtual machine stack',
    'evm compatible l1 and l2',
    'evm compatible layer 1s',
    'evm compatible layer 1s, 2s, and dapps',
    'evm compatible layer 2s',
    'solana virtual machine stack',
    'svm layer 1 and layer 2s',
    'svm layer 1s',
    'open source cryptography',
    'multichain infrastructure',
    'evm multichain dapps',
    'wallet (category)',
    'bridges & interoperability (category)',
    'oracles, data feeds, data providers (category)',
    'oracles (category)',
    'evm compatible application',
    'zero knowledge cryptography',
    'bridge (category)',
    'cosmos network stack',
    'polkadot network stack',
    'evm toolkit',
    'move stack'
    )

order by ltr.weighted_score_index desc