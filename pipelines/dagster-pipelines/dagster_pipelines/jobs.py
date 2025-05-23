from dagster import job, define_asset_job, AssetSelection, AssetsDefinition
import dagster as dg

# --- JOB FACTORY FOR COMMON PYTHON ASSETS ---
def create_env_specific_asset_job_from_prefixed(
    prefixed_asset_def: AssetsDefinition, # This is an already prefixed asset (e.g., stg_my_asset)
    base_job_name: str,                   # e.g., "project_repos_stargaze_count_refresh"
    base_description: str,
    tags: dict = None
):
    """
    Creates an environment-specific job for a single common Python asset
    that has already been prefixed.
    """
    # Infer env_prefix from the first part of the asset's key
    env_prefix = prefixed_asset_def.key.path[0] # Should be "stg" or "prod"

    return define_asset_job(
        name=f"{env_prefix}_{base_job_name}",
        selection=AssetSelection.assets(prefixed_asset_def), # Select the specific prefixed asset
        tags=tags if tags else {},
        description=f"[{env_prefix.upper()}] {base_description}"
    )

## ------------------------------------- JOBS FOR DBT ASSETS ------------------------------------- ##
# these do not use the factory because they are already environment specific
# Define a Dagster job for STAGING normalized dbt assets
stg_normalized_dbt_assets_job = define_asset_job(
    name="normalized_stg_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("stg_dbt_assets") & AssetSelection.tag("timestamp_normalized", ""),
    description="Runs STAGING data normalization dbt models with tag 'timestamp_normalized'."
)

# Define a Dagster job for STAGING latest clean data dbt assets
stg_latest_dbt_assets_job = define_asset_job(
    name="latest_stg_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("stg_dbt_assets") & AssetSelection.tag("latest_clean_data", ""),
    description="Runs STAGING dbt models to get latest clean data with tag 'latest_clean_data'."
)

# Define a Dagster job for STAGING period change data dbt assets
stg_period_change_data_dbt_assets_job = define_asset_job(
    name="period_change_stg_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("stg_dbt_assets") & AssetSelection.tag("period_change_data", ""),
    description="Runs STAGING dbt models for period change data with tag 'period_change_data'."
)

# Define a Dagster job for PROD normalized dbt assets
prod_normalized_dbt_assets_job = define_asset_job(
    name="normalized_prod_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("prod_dbt_assets") & AssetSelection.tag("timestamp_normalized", ""),
    description="Runs PROD data normalization dbt models with tag 'timestamp_normalized'."
)

# Define a Dagster job for PROD latest clean data dbt assets
prod_latest_dbt_assets_job = define_asset_job(
    name="latest_prod_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("prod_dbt_assets") & AssetSelection.tag("latest_clean_data", ""),
    description="Runs PROD dbt models to get latest clean data with tag 'latest_clean_data'."
)

# Define a Dagster job for PROD period change data dbt assets
prod_period_change_data_dbt_assets_job = define_asset_job(
    name="period_change_prod_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("prod_dbt_assets") & AssetSelection.tag("period_change_data", ""),
    description="Runs PROD dbt models for period change data with tag 'period_change_data'."
)





## ------------------------------------- JOBS FOR COMMON PYTHON ASSETS ------------------------------------- ##
### old method; replaced with job factory above

# # create a job to run update_repo_and_run_export asset
# update_crypto_ecosystems_repo_and_run_export_job = dg.define_asset_job(
#     "update_crypto_ecosystems_repo_and_run_export_refresh", 
#     selection=["update_crypto_ecosystems_repo_and_run_export"],
#     tags={"create_local_data_file": "True"},
#     description="Updates the crypto-ecosystems repo and runs the export script to create the local exports.jsonl file"
# )

# # create a job to run crypto_ecosystems_project_json asset
# crypto_ecosystems_project_json_job = dg.define_asset_job(
#     "crypto_ecosystems_project_json_refresh", 
#     selection=["crypto_ecosystems_project_json"],
#     description="Reads the local exports.jsonl file and creates a staging table in the raw schema"
# )

# # create a job to run latest_active_distinct_project_repos asset
# latest_active_distinct_project_repos_job = dg.define_asset_job(
#     "latest_active_distinct_project_repos_refresh", 
#     selection=["latest_active_distinct_github_project_repos"],
#     tags={"github_api": "True"},
#     description="Queries the latest distinct list of repos to check if the repo is still active and public. Overwrites the data in the raw.latest_active_distinct_github_project_repos table"
# )

# # create a job to run github_project_repos_stargaze_count asset
# project_repos_stargaze_count_job = dg.define_asset_job(
#     "project_repos_stargaze_count_refresh", 
#     selection=["github_project_repos_stargaze_count"],
#     tags={"github_api": "True"},
#     description="Gets the stargaze count for each repo from the github api and appends it to the raw.github_project_repos_stargaze_count table"
# )

# # create a job to run github_project_repos_fork_count asset
# project_repos_fork_count_job = dg.define_asset_job(
#     "project_repos_fork_count_refresh", 
#     selection=["github_project_repos_fork_count"],
#     tags={"github_api": "True"},
#     description="Gets the fork count for each repo from the github api and appends it to the raw.github_project_repos_fork_count table"
# )

# # create a job to run github_project_repos_languages asset
# project_repos_languages_job = dg.define_asset_job(
#     "project_repos_languages_refresh", 
#     selection=["github_project_repos_languages"],
#     tags={"github_api": "True"},
#     description="Gets the languages for each repo from the github api and appends it to the raw.github_project_repos_languages table"
# )

# # create a job to run github_project_repos_commits asset
# project_repos_commit_count_job = dg.define_asset_job(
#     "project_repos_commit_count_refresh", 
#     selection=["github_project_repos_commits"],
#     tags={"github_api": "True"},
#     description="Gets the commit count for each repo from the github api and appends it to the raw.github_project_repos_commits table"
# )

# # create a job to run github_project_repos_watchers asset
# project_repos_watcher_count_job = dg.define_asset_job(
#     "project_repos_watcher_count_refresh", 
#     selection=["github_project_repos_watcher_count"],
#     tags={"github_api": "True"},
#     description="Gets the watcher count for each repo from the github api and appends it to the raw.project_repos_watcher_count table"
# )

# # create a job to run github_project_repos_is_fork asset
# project_repos_is_fork_job = dg.define_asset_job(
#     "project_repos_is_fork_refresh", 
#     selection=["github_project_repos_is_fork"],
#     tags={"github_api": "True"},
#     description="Gets the is_fork for each repo from the github api and appends it to the raw.project_repos_is_fork table"
# )

# # create a job to run github_project_repos_contributors asset
# project_repos_contributors_job = dg.define_asset_job(
#     "project_repos_contributors_refresh", 
#     selection=["github_project_repos_contributors"],
#     tags={"github_api": "True"},
#     description="Gets the contributors for each repo from the github api, compresses the data and appends it to the raw.github_project_repos_contributors table"
# )

# # create a job to run process_compressed_contributors_data asset
# process_compressed_contributors_data_job = dg.define_asset_job(
#     "process_compressed_contributors_data_refresh", 
#     selection=["process_compressed_contributors_data"],
#     tags={"github_api": "True"},
#     description="Extracts, decompresses, and inserts data into the clean table. Only for the latest data in the raw.project_repos_contributors table. Performs checks for outliers before refreshing clean table."
# )

# # create a job to run latest_contributor_data asset
# latest_contributor_data_job = dg.define_asset_job(
#     "latest_contributor_data_refresh", 
#     selection=["latest_contributor_data"],
#     tags={"github_api": "True"},
#     description="Queries the latest list of contributors to check if the contributor is still active. Overwrites the data in the raw.latest_contributor_data table"
# )

# # create a job to run contributor_follower_counts asset
# contributor_follower_counts_job = dg.define_asset_job(
#     "contributor_follower_counts_refresh", 
#     selection=["contributor_follower_counts"],
#     tags={"github_api": "True"},
#     description="Queries the latest list of contributors to get follower count for each contributor. Appends the data in the raw.contributor_follower_counts table"
# )

# # create a job to run latest_contributor_following_count asset
# latest_contributor_following_count_job = dg.define_asset_job(
#     "latest_contributor_following_count_refresh", 
#     selection=["latest_contributor_following_count"],
#     tags={"github_api": "True"},
# )

# # create a job to run latest_contributor_activity asset
# latest_contributor_activity_job = dg.define_asset_job(
#     "latest_contributor_activity_refresh", 
#     selection=["latest_contributor_activity"],
#     tags={"github_api": "True"},
#     description="Queries the latest list of contributors to get recent activity for each contributor. Replaces the data in the raw.latest_contributor_activity table"
# )