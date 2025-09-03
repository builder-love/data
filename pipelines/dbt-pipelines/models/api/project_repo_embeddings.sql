-- models/api/project_repo_embeddings.sql

{{ 
    config(
        materialized='view',
        unique_key='project_title || repo || data_timestamp',
        tags=['api_data']
    ) 
}} 

select 
  repo,
  TO_CHAR(data_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS data_timestamp,
  corpus_embedding,
  project_title

from {{ source('prod_schema', 'latest_project_repo_corpus_embeddings_prod') }}