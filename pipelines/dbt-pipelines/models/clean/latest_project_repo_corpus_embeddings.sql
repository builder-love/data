-- models/clean/latest_project_repo_corpus_embeddings.sql
-- if clean.latest_project_repo_corpus_embeddings table exists, replace it with the raw.latest_project_repo_corpus_embeddings table
-- this is a simple replace, but the associated test checks record counts

{{ config(
    materialized='table',
    unique_key='project_title || repo || data_timestamp',
    tags=['latest_clean_data'],
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_hnsw_latest_project_repo_corpus_embeddings ON {{ this }} USING hnsw (corpus_embedding vector_cosine_ops)"
    ]
) }}

SELECT 
    embeddings.*, 
    projects.project_title

FROM {{ source('raw', 'latest_project_repo_corpus_embeddings') }} embeddings 
LEFT JOIN {{ ref('latest_project_repos')}} projects
    on embeddings.repo = projects.repo