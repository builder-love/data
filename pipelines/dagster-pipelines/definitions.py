import os

from dagster import Definitions, JobDefinition, ScheduleDefinition
from dagster_gcp.gcs import GCSPickleIOManager

# Import resources
from .resources import (
    cloud_sql_postgres_resource,
    dbt_stg_resource,
    dbt_prod_resource,
    active_env_config_resource,
    electric_capital_ecosystems_repo,
    gcs_storage_client_resource,
    github_api_resource
)
# import assets from assets.py
from .assets import ( # Adjust module path if they are in different files
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
    create_project_repos_readmes_asset,
    create_project_repos_package_files_asset,
    create_project_repos_app_dev_framework_files_asset,
    create_project_repos_frontend_framework_files_asset,
    create_project_repos_documentation_files_asset
)
# import assets from features.py
from .features import (
    create_project_repos_description_features_asset,
    create_project_repos_corpus_asset,
    create_project_repos_corpus_embeddings_asset
)
# import assets from models.py
from .models import (
    create_education_model_predictions_asset,
    create_scaffold_model_predictions_asset,
    create_developer_tooling_model_predictions_asset
)
from .cleaning_assets import ( 
    all_dbt_assets,
    create_process_compressed_contributors_data_asset
)
from .jobs import (
    create_env_specific_asset_job_from_prefixed,
    stg_normalized_dbt_assets_job,
    stg_latest_dbt_assets_job,
    stg_period_change_data_dbt_assets_job,
    prod_normalized_dbt_assets_job,
    prod_latest_dbt_assets_job,
    prod_period_change_data_dbt_assets_job,
    update_crypto_ecosystems_raw_file_job,
    prod_ml_pipeline_job,
    stg_ml_pipeline_job
)
from .schedules import (
    create_env_specific_schedule,
    refresh_prod_schema_schedule,
    refresh_api_schema_schedule,
    update_crypto_ecosystems_raw_file_schedule,
    stg_normalized_dbt_assets_schedule,
    stg_latest_dbt_assets_schedule,
    stg_period_change_data_dbt_assets_schedule,
    prod_normalized_dbt_assets_schedule,
    prod_latest_dbt_assets_schedule,
    prod_period_change_data_dbt_assets_schedule,
    prod_ml_pipeline_schedule,
    stg_ml_pipeline_schedule
)
from .load_data_jobs import (
    refresh_prod_schema, 
    create_update_crypto_ecosystems_repo_and_run_export_asset
)
from .api_data import refresh_api_schema

# --- Map base asset names to their job/schedule parameters ---
# Keys must match the keys in common_asset_creators.
asset_job_schedule_params_map = {
    "update_crypto_ecosystems_repo_and_run_export": {"base_job_name": "update_crypto_ecosystems_repo_and_run_export_refresh", "base_job_desc": "Updates the crypto-ecosystems repo...", "job_tags": {"create_local_data_file": "True"}, "cron_str": "0 19 * * 3", "base_schedule_name": "update_crypto_ecosystems_repo_and_run_export_schedule"},
    "crypto_ecosystems_project_json": {"base_job_name": "crypto_ecosystems_project_json_refresh", "base_job_desc": "Reads the local exports.jsonl file...", "cron_str": "0 20 * * 3", "base_schedule_name": "crypto_ecosystems_project_json_schedule"},
    "latest_active_distinct_github_project_repos": {"base_job_name": "latest_active_distinct_project_repos_refresh", "base_job_desc": "Queries the latest distinct list of repos...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 7,21 * *", "base_schedule_name": "latest_active_distinct_project_repos_schedule", "github_key_name": "github_finegrain_trebor"},
    "github_project_repos_stargaze_count": {"base_job_name": "project_repos_stargaze_count_refresh", "base_job_desc": "Gets the stargaze count...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 * * 0", "base_schedule_name": "project_repos_stargaze_count_schedule", "github_key_name": "github_finegrain_trebor"},
    "github_project_repos_fork_count": {"base_job_name": "project_repos_fork_count_refresh", "base_job_desc": "Gets the fork count...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 * * 6", "base_schedule_name": "project_repos_fork_count_schedule", "github_key_name": "github_finegrain_trebor"},
    "github_project_repos_languages": {"base_job_name": "project_repos_languages_refresh", "base_job_desc": "Gets the languages...", "job_tags": {"github_api_key1": "True"}, "cron_str": "0 0 16 * *", "base_schedule_name": "project_repos_languages_schedule", "github_key_name": "github_finegrain_trebor"},
    "github_project_repos_commits": {"base_job_name": "project_repos_commit_count_refresh", "base_job_desc": "Gets the commit count...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 * * 5", "base_schedule_name": "project_repos_commit_count_schedule", "github_key_name": "github_finegrain_trebor"},
    "github_project_repos_watcher_count": {"base_job_name": "project_repos_watcher_count_refresh", "base_job_desc": "Gets the watcher count...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 * * 1", "base_schedule_name": "project_repos_watcher_count_schedule", "github_key_name": "github_finegrain_trebor"},
    "github_project_repos_is_fork": {"base_job_name": "project_repos_is_fork_refresh", "base_job_desc": "Gets the is_fork status...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 * * 2", "base_schedule_name": "project_repos_is_fork_schedule", "github_key_name": "github_finegrain_trebor"},
    "github_project_repos_contributors": {"base_job_name": "project_repos_contributors_refresh", "base_job_desc": "Gets the contributors...", "job_tags": {"github_api_key2": "True"}, "cron_str": "0 0 20 * *", "base_schedule_name": "project_repos_contributors_schedule", "github_key_name": "github_finegrain_jackatj"},
    "project_repos_description": {"base_job_name": "project_repos_description_refresh", "base_job_desc": "Gets the repo description...", "job_tags": {"github_api_key1": "True"}, "cron_str": "0 0 26 * *", "base_schedule_name": "project_repos_description_schedule", "github_key_name": "github_finegrain_trebor"},
    "project_repos_readmes": {"base_job_name": "project_repos_readmes_refresh", "base_job_desc": "Gets the repo readmes...", "job_tags": {"github_api_key1": "True"}, "cron_str": "0 0 27 * *", "base_schedule_name": "project_repos_readmes_schedule", "github_key_name": "github_finegrain_trebor"},
    "project_repos_package_files": {"base_job_name": "project_repos_package_files_refresh", "base_job_desc": "Gets the repo package files...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 24 * *", "base_schedule_name": "project_repos_package_files_schedule", "github_key_name": "github_finegrain_trebor"},
    "project_repos_app_dev_framework_files": {"base_job_name": "project_repos_app_dev_framework_files_refresh", "base_job_desc": "Gets the repo app dev framework files...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 25 * *", "base_schedule_name": "project_repos_app_dev_framework_files_schedule", "github_key_name": "github_finegrain_trebor"},
    "project_repos_frontend_framework_files": {"base_job_name": "project_repos_frontend_framework_files_refresh", "base_job_desc": "Gets the repo frontend framework files...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 26 * *", "base_schedule_name": "project_repos_frontend_framework_files_schedule", "github_key_name": "github_finegrain_trebor"},
    "project_repos_documentation_files": {"base_job_name": "project_repos_documentation_files_refresh", "base_job_desc": "Gets the repo documentation files...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 27 * *", "base_schedule_name": "project_repos_documentation_files_schedule", "github_key_name": "github_finegrain_trebor"},
    "process_compressed_contributors_data": {"base_job_name": "process_compressed_contributors_data_refresh", "base_job_desc": "Extracts, decompresses, and inserts data...", "cron_str": "0 3 * * *", "base_schedule_name": "process_compressed_contributors_data_schedule"},
    "latest_contributor_data": {"base_job_name": "latest_contributor_data_refresh", "base_job_desc": "Queries the latest list of contributors...", "job_tags": {"github_api_key1": "True"}, "cron_str": "0 10 8,22 * *", "base_schedule_name": "latest_contributor_data_schedule", "github_key_name": "github_finegrain_trebor"},
    "contributor_follower_count": {"base_job_name": "contributor_follower_count_refresh", "base_job_desc": "Queries follower count...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 9 * *", "base_schedule_name": "contributor_follower_count_schedule", "github_key_name": "github_finegrain_trebor"},
    "latest_contributor_following_count": {"base_job_name": "latest_contributor_following_count_refresh", "base_job_desc": "Queries following count...", "job_tags": {"github_api_key1": "True"}, "cron_str": "10 0 12 * *", "base_schedule_name": "latest_contributor_following_count_schedule", "github_key_name": "github_finegrain_trebor"},
    "latest_contributor_activity": {"base_job_name": "latest_contributor_activity_refresh", "base_job_desc": "Queries recent activity...", "job_tags": {"github_api_key2": "True"}, "cron_str": "10 0 13 * *", "base_schedule_name": "latest_contributor_activity_schedule", "github_key_name": "github_finegrain_jackatj"},
    "project_repos_corpus_embeddings": {"base_job_name": "project_repos_corpus_embeddings_refresh", "base_job_desc": "Gets corpus embeddings from gcs...", "job_tags": {"ml_ops": "True"}, "cron_str": "0 0 28 * *", "base_schedule_name": "project_repos_corpus_embeddings_schedule"},
}

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
    "project_repos_package_files": create_project_repos_package_files_asset,
    "project_repos_app_dev_framework_files": create_project_repos_app_dev_framework_files_asset,
    "project_repos_frontend_framework_files": create_project_repos_frontend_framework_files_asset,
    "project_repos_description_features": create_project_repos_description_features_asset,
    "project_repos_documentation_files": create_project_repos_documentation_files_asset,
    "project_repos_corpus": create_project_repos_corpus_asset,
    "project_repos_corpus_embeddings": create_project_repos_corpus_embeddings_asset,
    "education_model_predictions": create_education_model_predictions_asset,
    "scaffold_model_predictions": create_scaffold_model_predictions_asset,
    "developer_tooling_model_predictions": create_developer_tooling_model_predictions_asset,
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
        # The creator function no longer takes 'config'
        stg_asset = creator_fn(env_prefix="stg")
        stg_prefixed_common_assets.append(stg_asset)
        stg_assets_by_base_name[base_name] = stg_asset

        prod_asset = creator_fn(env_prefix="prod")
        prod_prefixed_common_assets.append(prod_asset)
        prod_assets_by_base_name[base_name] = prod_asset
    except Exception as e:
        print(f"Unexpected error creating asset for base_name '{base_name}': {e}")
        raise

## ------------------------------------------ create jobs and schedules for each environment --------------------------------- ##
# --- Generate Staging Jobs and Schedules ---
stg_jobs_list = [stg_normalized_dbt_assets_job, stg_latest_dbt_assets_job, stg_period_change_data_dbt_assets_job, stg_ml_pipeline_job]
stg_schedules_list = [stg_normalized_dbt_assets_schedule, stg_latest_dbt_assets_schedule, stg_period_change_data_dbt_assets_schedule, stg_ml_pipeline_schedule]

for base_name, params in asset_job_schedule_params_map.items():
    if base_name in stg_assets_by_base_name:
        stg_asset_to_job = stg_assets_by_base_name[base_name]

        # Build the config dictionary specifically for this job
        job_config = {}
        if "github_key_name" in params:
            job_config = {
                "ops": {
                    # This gets the underlying op name for the asset, e.g., "stg__my_asset"
                    stg_asset_to_job.op.name: {
                        "config": {
                            "key_name": params["github_key_name"]
                        }
                    }
                }
            }

        stg_job = create_env_specific_asset_job_from_prefixed(
            prefixed_asset_def=stg_asset_to_job,
            base_job_name=params["base_job_name"],
            base_description=params["base_job_desc"],
            tags=params.get("job_tags", {}), # Use .get for safety
            config=job_config  # Pass the constructed config to the job helper
        )
        stg_jobs_list.append(stg_job)
        
        stg_schedule = create_env_specific_schedule(stg_job, params["cron_str"], params["base_schedule_name"])
        stg_schedules_list.append(stg_schedule)
    else:
        # A warning if an asset in the map doesn't have a creator function
        print(f"Warning: Asset creator for base_name '{base_name}' not found in stg_assets_by_base_name during STG job/schedule creation.")

# Add one-off jobs/schedules to STAGING
if 'update_crypto_ecosystems_raw_file_job' in globals() and isinstance(update_crypto_ecosystems_raw_file_job, JobDefinition):
    stg_jobs_list.append(update_crypto_ecosystems_raw_file_job)
    if 'update_crypto_ecosystems_raw_file_schedule' in globals() and isinstance(update_crypto_ecosystems_raw_file_schedule, ScheduleDefinition): stg_schedules_list.append(update_crypto_ecosystems_raw_file_schedule)
if 'refresh_prod_schema' in globals() and isinstance(refresh_prod_schema, JobDefinition):
    stg_jobs_list.append(refresh_prod_schema)
    if 'refresh_prod_schema_schedule' in globals() and isinstance(refresh_prod_schema_schedule, ScheduleDefinition): stg_schedules_list.append(refresh_prod_schema_schedule)
if 'refresh_api_schema' in globals() and isinstance(refresh_api_schema, JobDefinition):
    stg_jobs_list.append(refresh_api_schema)
    if 'refresh_api_schema_schedule' in globals() and isinstance(refresh_api_schema_schedule, ScheduleDefinition): stg_schedules_list.append(refresh_api_schema_schedule)


# --- Generate Production Jobs and Schedules ---
prod_jobs_list = [prod_normalized_dbt_assets_job, prod_latest_dbt_assets_job, prod_period_change_data_dbt_assets_job, prod_ml_pipeline_job]
prod_schedules_list = [prod_normalized_dbt_assets_schedule, prod_latest_dbt_assets_schedule, prod_period_change_data_dbt_assets_schedule, prod_ml_pipeline_schedule]

for base_name, params in asset_job_schedule_params_map.items():
    if base_name in prod_assets_by_base_name:
        prod_asset_to_job = prod_assets_by_base_name[base_name]

        job_config = {}
        if "github_key_name" in params:
            job_config = {
                "ops": {
                    prod_asset_to_job.op.name: {
                        "config": {
                            "key_name": params["github_key_name"]
                        }
                    }
                }
            }

        prod_job = create_env_specific_asset_job_from_prefixed(
            prefixed_asset_def=prod_asset_to_job,
            base_job_name=params["base_job_name"],
            base_description=params["base_job_desc"],
            tags=params.get("job_tags", {}),
            config=job_config
        )
        prod_jobs_list.append(prod_job)
        
        prod_schedule = create_env_specific_schedule(prod_job, params["cron_str"], params["base_schedule_name"])
        prod_schedules_list.append(prod_schedule)
    else:
        print(f"Warning: Asset creator for base_name '{base_name}' not found in prod_assets_by_base_name during PROD job/schedule creation.")

# Add one-off jobs/schedules to PRODUCTION
if 'update_crypto_ecosystems_raw_file_job' in globals() and isinstance(update_crypto_ecosystems_raw_file_job, JobDefinition):
    prod_jobs_list.append(update_crypto_ecosystems_raw_file_job)
    if 'update_crypto_ecosystems_raw_file_schedule' in globals() and isinstance(update_crypto_ecosystems_raw_file_schedule, ScheduleDefinition): prod_schedules_list.append(update_crypto_ecosystems_raw_file_schedule)
if 'refresh_prod_schema' in globals() and isinstance(refresh_prod_schema, JobDefinition):
    prod_jobs_list.append(refresh_prod_schema)
    if 'refresh_prod_schema_schedule' in globals() and isinstance(refresh_prod_schema_schedule, ScheduleDefinition): prod_schedules_list.append(refresh_prod_schema_schedule)
if 'refresh_api_schema' in globals() and isinstance(refresh_api_schema, JobDefinition):
    prod_jobs_list.append(refresh_api_schema)
    if 'refresh_api_schema_schedule' in globals() and isinstance(refresh_api_schema_schedule, ScheduleDefinition): prod_schedules_list.append(refresh_api_schema_schedule)

## ------------------------------------------ Environment-based Definitions Builder --------------------------------- ##

# Read the environment variable to determine which assets, jobs, and resources to use.
DAGSTER_ENV = os.getenv("DAGSTER_ENV", "stg") # Default to 'stg'

# Set up environment-specific components based on the environment variable
if DAGSTER_ENV == "prod":
    prefixed_common_assets = prod_prefixed_common_assets
    jobs_list = prod_jobs_list
    schedules_list = prod_schedules_list
    active_dbt_resource = dbt_prod_resource
    active_env_target = "prod"
else: # Default to staging
    prefixed_common_assets = stg_prefixed_common_assets
    jobs_list = stg_jobs_list
    schedules_list = stg_schedules_list
    active_dbt_resource = dbt_stg_resource
    active_env_target = "stg"

# Combine the Python-defined assets with the dynamically loaded dbt assets.
# The 'if all_dbt_assets' check handles the case where the manifest was not found.
final_assets = prefixed_common_assets + ([all_dbt_assets] if all_dbt_assets else [])

# Determine if a keyfile path is provided. This will be true locally and false in GKE.
gcp_keyfile = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
gcs_resource_config = {"gcp_keyfile_path": gcp_keyfile} if gcp_keyfile else {}
# Create the shared GCS resource instance that all parts of the app will use
shared_gcs_resource = gcs_storage_client_resource(**gcs_resource_config)

# Define a single, unified Definitions object
defs = Definitions(
    assets=final_assets,
    jobs=jobs_list,
    schedules=schedules_list,
    resources={
        # This is the key the @dbt_assets decorator looks for.
        "dbt_cli": active_dbt_resource,

        # Configure other resources dynamically
        "active_env_config": active_env_config_resource.configured({"env_target": active_env_target}),

        # Keep other resources that might be used by name
        "cloud_sql_postgres_resource": cloud_sql_postgres_resource.configured(
            {
                "username": os.getenv("cloud_sql_user"),
                "password": os.getenv("cloud_sql_password"),
                "hostname": os.getenv("cloud_sql_postgres_host"),
                "database": os.getenv("cloud_sql_postgres_db"),
            }
        ),
        "dbt_stg_resource": dbt_stg_resource,
        "dbt_prod_resource": dbt_prod_resource,
        "electric_capital_ecosystems_repo": electric_capital_ecosystems_repo,
        "gcs": shared_gcs_resource,
        # Configure the IO Manager by passing the shared resource object directly
        "io_manager": GCSPickleIOManager(
            gcs_bucket="bl-dagster-io-storage",
            gcs_prefix=active_env_target,
            gcs=shared_gcs_resource  # <-- Pass the object directly
        ),
        "github_api": github_api_resource(),
    },
)