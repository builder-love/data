from dagster import job, define_asset_job, AssetSelection, AssetsDefinition, AssetKey
from .cleaning_assets import (
    run_dbt_tests_on_crypto_ecosystems_raw_file_staging, 
    load_new_data_from_staging_to_final, 
    update_projects_dimension_and_archive,
    update_repos_dimension_and_archive
)
# import assets from features.py
from .features import (
    create_project_repos_description_features_asset
)
# import assets from models.py
from .models import (
    create_education_model_predictions_asset,
    create_scaffold_model_predictions_asset,
    create_developer_tooling_model_predictions_asset
)
import dagster as dg

# --- JOB FACTORY FOR COMMON PYTHON ASSETS ---
def create_env_specific_asset_job_from_prefixed(
    prefixed_asset_def: AssetsDefinition, # This is an already prefixed asset (e.g., stg_my_asset)
    base_job_name: str,                   # e.g., "project_repos_stargaze_count_refresh"
    base_description: str,
    tags: dict = None,
    config: dict = None 
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
        description=f"[{env_prefix.upper()}] {base_description}",
        config=config
    )

## ------------------------------------- JOBS FOR CRYPTO ECOSYSTEMS ------------------------------------- ##
# run the raw file job
# if successful, run the projects dimension job
@job()
def update_crypto_ecosystems_raw_file_job():
    """
    Tests the staging data and then updates the crypto_ecosystems_raw_file table.
    """
    # Execute tests first. If this op raises an Exception (fails), the job stops.
    test_results_passed = run_dbt_tests_on_crypto_ecosystems_raw_file_staging()

    # Define that load_new_data depends on the test results.
    # Pass the boolean result just for completeness or potential internal checks,
    # but the main control flow comes from the dependency + potential failure in the test op.
    main_data_updated_signal = load_new_data_from_staging_to_final(test_results_passed)

    # run the projects dimension job
    projects_dimension_updated_signal = update_projects_dimension_and_archive(previous_op_result=main_data_updated_signal)

    # run the repos dimension job
    update_repos_dimension_and_archive(after_projects_updated=projects_dimension_updated_signal)
## ------------------------------------- JOBS FOR CRYPTO ECOSYSTEMS ------------------------------------- ##

## ------------------------------------- JOBS FOR DBT ASSETS ------------------------------------- ##
# these do not use the factory because they are already environment specific
# Define a Dagster job for STAGING normalized dbt assets
stg_normalized_dbt_assets_job = define_asset_job(
    name="normalized_stg_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("dbt_stg") & AssetSelection.tag("timestamp_normalized", ""),
    description="Runs STAGING data normalization dbt models with tag 'timestamp_normalized'.",
    tags={
        "nightly_dbt_model_lock": "True"
    }
)

# Define a Dagster job for STAGING latest clean data dbt assets
stg_latest_dbt_assets_job = define_asset_job(
    name="latest_stg_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("dbt_stg") & AssetSelection.tag("latest_clean_data", ""),
    description="Runs STAGING dbt models to get latest clean data with tag 'latest_clean_data'.",
    tags={
        "nightly_dbt_model_lock": "True"
    }
)

# Define a Dagster job for STAGING period change data dbt assets
stg_period_change_data_dbt_assets_job = define_asset_job(
    name="period_change_stg_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("dbt_stg") & AssetSelection.tag("period_change_data", ""),
    description="Runs STAGING dbt models for period change data with tag 'period_change_data'.",
    tags={
        "nightly_dbt_model_lock": "True"
    }
)

# Define a Dagster job for PROD normalized dbt assets
prod_normalized_dbt_assets_job = define_asset_job(
    name="normalized_prod_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("dbt_prod") & AssetSelection.tag("timestamp_normalized", ""),
    description="Runs PROD data normalization dbt models with tag 'timestamp_normalized'.",
    tags={
        "nightly_dbt_model_lock": "True"
    }
)

# Define a Dagster job for PROD latest clean data dbt assets
prod_latest_dbt_assets_job = define_asset_job(
    name="latest_prod_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("dbt_prod") & AssetSelection.tag("latest_clean_data", ""),
    description="Runs PROD dbt models to get latest clean data with tag 'latest_clean_data'.",
    tags={
        "nightly_dbt_model_lock": "True"
    }
)

# Define a Dagster job for PROD period change data dbt assets
prod_period_change_data_dbt_assets_job = define_asset_job(
    name="period_change_prod_dbt_assets_job", # Renamed for clarity
    selection=AssetSelection.groups("dbt_prod") & AssetSelection.tag("period_change_data", ""),
    description="Runs PROD dbt models for period change data with tag 'period_change_data'.",
    tags={
        "nightly_dbt_model_lock": "True"
    }
)

## ------------------------------------- JOB FOR THE FULL ML PIPELINE ------------------------------------- ##
# This job materializes the feature asset and all downstream model assets.
# It's defined once and can be used in both stg and prod definitions.
ML_PIPELINE_UPSTREAM_ASSET_NAME = "project_repos_description_features"

# This selection is for the PRODUCTION environment
prod_asset_key = AssetKey(["prod", ML_PIPELINE_UPSTREAM_ASSET_NAME])
prod_ml_pipeline_selection = (
    AssetSelection.assets(prod_asset_key) | # select the root
    AssetSelection.assets(prod_asset_key).downstream(depth=2) # select downstream to all depths
)

# This defines the job for the PRODUCTION environment
prod_ml_pipeline_job = define_asset_job(
    name="prod_full_ml_pipeline_job",
    selection=prod_ml_pipeline_selection, # Use the prod-specific selection
    description="Refreshes features and runs all downstream ML models for PROD."
)

# This selection is for the STAGING environment
stg_asset_key = AssetKey(["stg", ML_PIPELINE_UPSTREAM_ASSET_NAME])
stg_ml_pipeline_selection = (
    AssetSelection.assets(stg_asset_key) | # select the root
    AssetSelection.assets(stg_asset_key).downstream(depth=2) # select downstream to all depths
)

# This defines the job for the STAGING environment
stg_ml_pipeline_job = define_asset_job(
    name="stg_full_ml_pipeline_job",
    selection=stg_ml_pipeline_selection, # Use the stg-specific selection
    description="Refreshes features and runs all downstream ML models for STG."
)

## ------------------------------------- JOB FOR THE FULL ML PIPELINE ------------------------------------- ##