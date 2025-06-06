import os

from dagster import Definitions, with_resources, AssetSelection, define_asset_job, JobDefinition, ScheduleDefinition, AssetKey, asset, OpExecutionContext
# Import resources
from dagster_pipelines.resources import (
    cloud_sql_postgres_resource,
    dbt_stg_resource,
    dbt_prod_resource,
    active_env_config_resource,
    electric_capital_ecosystems_repo
)

from dagster_pipelines.assets import ( # Adjust module path if they are in different files
    create_crypto_ecosystems_project_json_asset,
    create_latest_active_distinct_github_project_repos_asset,
    create_github_project_repos_stargaze_count_asset,
    create_github_project_repos_fork_count_asset,
    create_github_project_repos_languages_asset,
    create_github_project_repos_commits_asset,
    create_github_project_repos_watcher_count_asset,
    create_github_project_repos_is_fork_asset,
    create_github_project_repos_contributors_asset,
    create_latest_contributor_data_asset,
    create_contributor_follower_count_asset,
    create_latest_contributor_following_count_asset,
    create_latest_contributor_activity_asset,
    create_project_repos_description_asset,
    create_project_repos_readmes_asset
)
from dagster_pipelines.features import (
    create_project_repos_description_features_asset
)
from dagster_pipelines.cleaning_assets import ( 
    all_stg_dbt_assets, 
    all_prod_dbt_assets,
    create_process_compressed_contributors_data_asset
)
from dagster_pipelines.jobs import (
    create_env_specific_asset_job_from_prefixed,
    stg_normalized_dbt_assets_job,
    stg_latest_dbt_assets_job,
    stg_period_change_data_dbt_assets_job,
    prod_normalized_dbt_assets_job,
    prod_latest_dbt_assets_job,
    prod_period_change_data_dbt_assets_job,
    update_crypto_ecosystems_raw_file_job
)
from dagster_pipelines.schedules import (
    create_env_specific_schedule,
    refresh_prod_schema_schedule,
    refresh_api_schema_schedule,
    update_crypto_ecosystems_raw_file_schedule,
    stg_normalized_dbt_assets_schedule,
    stg_latest_dbt_assets_schedule,
    stg_period_change_data_dbt_assets_schedule,
    prod_normalized_dbt_assets_schedule,
    prod_latest_dbt_assets_schedule,
    prod_period_change_data_dbt_assets_schedule
)
from dagster_pipelines.load_data_jobs import (
    refresh_prod_schema, 
    create_update_crypto_ecosystems_repo_and_run_export_asset
)
from dagster_pipelines.api_data import refresh_api_schema

## ------------------------------------------ end imports --------------------------------- ##

# Bind the correct dbt resource to each group of dbt assets.
# The @dbt_assets decorator makes assets look for a resource named "dbt_cli".
stg_dbt_assets_with_resource = with_resources(
    [all_stg_dbt_assets],
    resource_defs={"dbt_cli": dbt_stg_resource}
)

prod_dbt_assets_with_resource = with_resources(
    [all_prod_dbt_assets],
    resource_defs={"dbt_cli": dbt_prod_resource}
)

## ------------------------------------------ define common assets, jobs, and schedules --------------------------------- ##
# --- Define mapping from base asset names to their CREATOR functions ---
# The keys here MUST match the 'name' argument given in the @asset decorator
# inside the respective factory functions.
common_asset_creators = {
    "process_compressed_contributors_data": create_process_compressed_contributors_data_asset,
    "crypto_ecosystems_project_json": create_crypto_ecosystems_project_json_asset,
    "latest_active_distinct_github_project_repos": create_latest_active_distinct_github_project_repos_asset,
    "github_project_repos_stargaze_count": create_github_project_repos_stargaze_count_asset,
    "github_project_repos_fork_count": create_github_project_repos_fork_count_asset,
    "github_project_repos_languages": create_github_project_repos_languages_asset,
    "github_project_repos_commits": create_github_project_repos_commits_asset,
    "github_project_repos_watcher_count": create_github_project_repos_watcher_count_asset,
    "github_project_repos_is_fork": create_github_project_repos_is_fork_asset,
    "github_project_repos_contributors": create_github_project_repos_contributors_asset,
    "project_repos_description": create_project_repos_description_asset,
    "project_repos_readmes": create_project_repos_readmes_asset,
    "project_repos_description_features": create_project_repos_description_features_asset,
    "latest_contributor_data": create_latest_contributor_data_asset,
    "contributor_follower_count": create_contributor_follower_count_asset,
    "latest_contributor_following_count": create_latest_contributor_following_count_asset,
    "latest_contributor_activity": create_latest_contributor_activity_asset,
    "update_crypto_ecosystems_repo_and_run_export": create_update_crypto_ecosystems_repo_and_run_export_asset,
}

# --- Create Staging and Production versions of common assets ---
stg_prefixed_common_assets = []
prod_prefixed_common_assets = []
stg_assets_by_base_name = {} # Helper map for jobs/schedules
prod_assets_by_base_name = {}

for base_name, creator_fn in common_asset_creators.items():
    try:
        stg_asset = creator_fn(env_prefix="stg")
        stg_prefixed_common_assets.append(stg_asset)
        stg_assets_by_base_name[base_name] = stg_asset

        prod_asset = creator_fn(env_prefix="prod")
        prod_prefixed_common_assets.append(prod_asset)
        prod_assets_by_base_name[base_name] = prod_asset
    except TypeError as e:
        # This might happen if a creator function is not found or is not callable
        print(f"Error calling creator function for base_name '{base_name}': {e}. Ensure it's imported and correct in common_asset_creators.")
        raise
    except Exception as e:
        print(f"Unexpected error creating asset for base_name '{base_name}': {e}")
        raise

# --- Map base asset names to their job/schedule parameters ---
# Keys must match the keys in common_asset_creators.
asset_job_schedule_params_map = {
    "update_crypto_ecosystems_repo_and_run_export": {"base_job_name": "update_crypto_ecosystems_repo_and_run_export_refresh", "base_job_desc": "Updates the crypto-ecosystems repo...", "job_tags": {"create_local_data_file": "True"}, "cron_str": "0 19 * * 3", "base_schedule_name": "update_crypto_ecosystems_repo_and_run_export_schedule"},
    "crypto_ecosystems_project_json": {"base_job_name": "crypto_ecosystems_project_json_refresh", "base_job_desc": "Reads the local exports.jsonl file...", "job_tags": {}, "cron_str": "0 20 * * 3", "base_schedule_name": "crypto_ecosystems_project_json_schedule"},
    "latest_active_distinct_github_project_repos": {"base_job_name": "latest_active_distinct_project_repos_refresh", "base_job_desc": "Queries the latest distinct list of repos...", "job_tags": {"github_api": "True"}, "cron_str": "10 0 7,21 * *", "base_schedule_name": "latest_active_distinct_project_repos_schedule"},
    "github_project_repos_stargaze_count": {"base_job_name": "project_repos_stargaze_count_refresh", "base_job_desc": "Gets the stargaze count...", "job_tags": {"github_api": "True"}, "cron_str": "10 0 * * 0", "base_schedule_name": "project_repos_stargaze_count_schedule"},
    "github_project_repos_fork_count": {"base_job_name": "project_repos_fork_count_refresh", "base_job_desc": "Gets the fork count...", "job_tags": {"github_api": "True"}, "cron_str": "10 0 * * 6", "base_schedule_name": "project_repos_fork_count_schedule"},
    "github_project_repos_languages": {"base_job_name": "project_repos_languages_refresh", "base_job_desc": "Gets the languages...", "job_tags": {"github_api": "True"}, "cron_str": "0 0 16 * *", "base_schedule_name": "project_repos_languages_schedule"},
    "github_project_repos_commits": {"base_job_name": "project_repos_commit_count_refresh", "base_job_desc": "Gets the commit count...", "job_tags": {"github_api": "True"}, "cron_str": "10 0 * * 5", "base_schedule_name": "project_repos_commit_count_schedule"},
    "github_project_repos_watcher_count": {"base_job_name": "project_repos_watcher_count_refresh", "base_job_desc": "Gets the watcher count...", "job_tags": {"github_api": "True"}, "cron_str": "10 0 * * 1", "base_schedule_name": "project_repos_watcher_count_schedule"},
    "github_project_repos_is_fork": {"base_job_name": "project_repos_is_fork_refresh", "base_job_desc": "Gets the is_fork status...", "job_tags": {"github_api": "True"}, "cron_str": "10 0 * * 2", "base_schedule_name": "project_repos_is_fork_schedule"},
    "github_project_repos_contributors": {"base_job_name": "project_repos_contributors_refresh", "base_job_desc": "Gets the contributors...", "job_tags": {"github_api": "True"}, "cron_str": "0 0 20 * *", "base_schedule_name": "project_repos_contributors_schedule"},
    "project_repos_description": {"base_job_name": "project_repos_description_refresh", "base_job_desc": "Gets the repo description...", "job_tags": {"github_api": "True"}, "cron_str": "0 0 26 * *", "base_schedule_name": "project_repos_description_schedule"},
    "project_repos_readmes": {"base_job_name": "project_repos_readmes_refresh", "base_job_desc": "Gets the repo readmes...", "job_tags": {"github_api": "True"}, "cron_str": "0 0 27 * *", "base_schedule_name": "project_repos_readmes_schedule"},
    "project_repos_description_features": {"base_job_name": "project_repos_description_features_refresh", "base_job_desc": "Gets the repo description features...", "job_tags": {"github_api": "True"}, "cron_str": "0 0 28 * *", "base_schedule_name": "project_repos_description_features_schedule"},
    "process_compressed_contributors_data": {"base_job_name": "process_compressed_contributors_data_refresh", "base_job_desc": "Extracts, decompresses, and inserts data...", "job_tags": {"github_api": "True"}, "cron_str": "0 3 * * *", "base_schedule_name": "process_compressed_contributors_data_schedule"},
    "latest_contributor_data": {"base_job_name": "latest_contributor_data_refresh", "base_job_desc": "Queries the latest list of contributors...", "job_tags": {"github_api": "True"}, "cron_str": "0 10 8,22 * *", "base_schedule_name": "latest_contributor_data_schedule"},
    "contributor_follower_count": {"base_job_name": "contributor_follower_count_refresh", "base_job_desc": "Queries follower count...", "job_tags": {"github_api": "True"}, "cron_str": "10 0 9 * *", "base_schedule_name": "contributor_follower_count_schedule"},
    "latest_contributor_following_count": {"base_job_name": "latest_contributor_following_count_refresh", "base_job_desc": "Queries following count...", "job_tags": {"github_api": "True"}, "cron_str": "10 0 12 * *", "base_schedule_name": "latest_contributor_following_count_schedule"},
    "latest_contributor_activity": {"base_job_name": "latest_contributor_activity_refresh", "base_job_desc": "Queries recent activity...", "job_tags": {"github_api": "True"}, "cron_str": "10 0 13 * *", "base_schedule_name": "latest_contributor_activity_schedule"},
}


## ------------------------------------------ create jobs and schedules for each environment --------------------------------- ##
# --- Generate Staging Jobs and Schedules ---
stg_jobs_list = [stg_normalized_dbt_assets_job, stg_latest_dbt_assets_job, stg_period_change_data_dbt_assets_job]
stg_schedules_list = [stg_normalized_dbt_assets_schedule, stg_latest_dbt_assets_schedule, stg_period_change_data_dbt_assets_schedule]

for base_name, params in asset_job_schedule_params_map.items():
    if base_name in stg_assets_by_base_name:
        stg_asset_to_job = stg_assets_by_base_name[base_name]

        stg_job = create_env_specific_asset_job_from_prefixed(
            prefixed_asset_def=stg_asset_to_job,
            base_job_name=params["base_job_name"],
            base_description=params["base_job_desc"],
            tags=params["job_tags"]
        )
        stg_jobs_list.append(stg_job)
        stg_schedule = create_env_specific_schedule(stg_job, params["cron_str"], params["base_schedule_name"])
        stg_schedules_list.append(stg_schedule)
    else:
        print(f"Warning: Asset creator for base_name '{base_name}' not found in stg_assets_by_base_name during STG job/schedule creation. Ensure it's in common_asset_creators and asset_job_schedule_params_map.")

# Add one-off jobs/schedules to STAGING (as per your decision for them to be in both envs)
# (Copied from previous correct version, ensure imports are correct)
if 'update_crypto_ecosystems_raw_file_job' in globals() and isinstance(update_crypto_ecosystems_raw_file_job, JobDefinition):
    stg_jobs_list.append(update_crypto_ecosystems_raw_file_job)
    if 'update_crypto_ecosystems_raw_file_schedule' in globals() and isinstance(update_crypto_ecosystems_raw_file_schedule, ScheduleDefinition): stg_schedules_list.append(update_crypto_ecosystems_raw_file_schedule)
if 'refresh_prod_schema' in globals() and isinstance(refresh_prod_schema, JobDefinition): # Must act on STG schema here
    stg_jobs_list.append(refresh_prod_schema)
    if 'refresh_prod_schema_schedule' in globals() and isinstance(refresh_prod_schema_schedule, ScheduleDefinition): stg_schedules_list.append(refresh_prod_schema_schedule)
if 'refresh_api_schema' in globals() and isinstance(refresh_api_schema, JobDefinition):
    stg_jobs_list.append(refresh_api_schema)
    if 'refresh_api_schema_schedule' in globals() and isinstance(refresh_api_schema_schedule, ScheduleDefinition): stg_schedules_list.append(refresh_api_schema_schedule)


# --- Generate Production Jobs and Schedules ---
prod_jobs_list = [prod_normalized_dbt_assets_job, prod_latest_dbt_assets_job, prod_period_change_data_dbt_assets_job]
prod_schedules_list = [prod_normalized_dbt_assets_schedule, prod_latest_dbt_assets_schedule, prod_period_change_data_dbt_assets_schedule]

for base_name, params in asset_job_schedule_params_map.items():
    if base_name in prod_assets_by_base_name:
        prod_asset_to_job = prod_assets_by_base_name[base_name]
        prod_job = create_env_specific_asset_job_from_prefixed(
            prefixed_asset_def=prod_asset_to_job,
            base_job_name=params["base_job_name"],
            base_description=params["base_job_desc"],
            tags=params["job_tags"]
        )
        prod_jobs_list.append(prod_job)
        prod_schedule = create_env_specific_schedule(prod_job, params["cron_str"], params["base_schedule_name"])
        prod_schedules_list.append(prod_schedule)
    else:
        print(f"Warning: Asset creator for base_name '{base_name}' not found in prod_assets_by_base_name during PROD job/schedule creation. Ensure it's in common_asset_creators and asset_job_schedule_params_map.")

# Add one-off jobs/schedules to PRODUCTION (as per your decision for them to be in both envs)
# (Copied from previous correct version, ensure imports are correct)
if 'update_crypto_ecosystems_raw_file_job' in globals() and isinstance(update_crypto_ecosystems_raw_file_job, JobDefinition):
    prod_jobs_list.append(update_crypto_ecosystems_raw_file_job)
    if 'update_crypto_ecosystems_raw_file_schedule' in globals() and isinstance(update_crypto_ecosystems_raw_file_schedule, ScheduleDefinition): prod_schedules_list.append(update_crypto_ecosystems_raw_file_schedule)
if 'refresh_prod_schema' in globals() and isinstance(refresh_prod_schema, JobDefinition):
    prod_jobs_list.append(refresh_prod_schema)
    if 'refresh_prod_schema_schedule' in globals() and isinstance(refresh_prod_schema_schedule, ScheduleDefinition): prod_schedules_list.append(refresh_prod_schema_schedule)
if 'refresh_api_schema' in globals() and isinstance(refresh_api_schema, JobDefinition):
    prod_jobs_list.append(refresh_api_schema)
    if 'refresh_api_schema_schedule' in globals() and isinstance(refresh_api_schema_schedule, ScheduleDefinition): prod_schedules_list.append(refresh_api_schema_schedule)

## ------------------------------------------ create defintion objects for each environment --------------------------------- ##
# --- Production Specific Definitions ---
prod_specific_defs = Definitions(
    resources={
        "cloud_sql_postgres_resource": cloud_sql_postgres_resource.configured(
            {
                "username": os.getenv("cloud_sql_user"),
                "password": os.getenv("cloud_sql_password"),
                "hostname": os.getenv("cloud_sql_postgres_host"),
                "database": os.getenv("cloud_sql_postgres_db"),
            }
        ),
        "active_env_config": active_env_config_resource.configured({"env_target": "prod"}),
        "active_dbt_runner": dbt_prod_resource,  # Generic ops use PROD dbt runner
        "dbt_stg_resource": dbt_stg_resource,    # For stg_dbt_assets or stg-specific jobs
        "dbt_prod_resource": dbt_prod_resource,   # For prod_dbt_assets or prod-specific jobs
        "electric_capital_ecosystems_repo": electric_capital_ecosystems_repo
    },
    assets=prod_prefixed_common_assets + list(prod_dbt_assets_with_resource),
    jobs=prod_jobs_list,
    schedules=prod_schedules_list,
)

# --- Staging Specific Definitions ---
stg_specific_defs = Definitions(
    resources={
        "cloud_sql_postgres_resource": cloud_sql_postgres_resource.configured(
            {
                "username": os.getenv("cloud_sql_user"),
                "password": os.getenv("cloud_sql_password"),
                "hostname": os.getenv("cloud_sql_postgres_host"),
                "database": os.getenv("cloud_sql_postgres_db"),
            }
        ),
        "active_env_config": active_env_config_resource.configured({"env_target": "stg"}),
        "active_dbt_runner": dbt_stg_resource,   # Generic ops use STG dbt runner
        "dbt_stg_resource": dbt_stg_resource,    # For stg_dbt_assets or stg-specific jobs
        "dbt_prod_resource": dbt_prod_resource,   # Still available if needed, though less common for stg defs
        "electric_capital_ecosystems_repo": electric_capital_ecosystems_repo
    },
    assets=stg_prefixed_common_assets + list(stg_dbt_assets_with_resource),
    jobs=stg_jobs_list,
    schedules=stg_schedules_list,
)