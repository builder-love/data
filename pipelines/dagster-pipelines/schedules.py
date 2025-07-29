from dagster import schedule, JobDefinition
from .jobs import (
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
from .load_data_jobs import refresh_prod_schema
from .api_data import refresh_api_schema


# --- SCHEDULE FACTORY FOR COMMON PYTHON ASSET JOBS ---
def create_env_specific_schedule(
    env_specific_job: JobDefinition,
    cron_schedule_str: str,
    base_schedule_name: str, # e.g., "project_repos_stargaze_count_schedule"
    execution_timezone: str = "America/New_York"
):
    """
    Creates an environment-specific schedule for a given environment-specific job.
    The schedule name will be prefixed based on the job's prefix.
    """
    # Infer environment prefix from the job's name (e.g., "stg_" or "prod_")
    job_name_parts = env_specific_job.name.split("_", 1)
    env_prefix = job_name_parts[0] if len(job_name_parts) > 1 and job_name_parts[0] in ["stg", "prod"] else "generic"
    
    # Ensure unique schedule name by incorporating the prefix
    schedule_name = f"{env_prefix}_{base_schedule_name}"

    @schedule(
        job=env_specific_job,
        cron_schedule=cron_schedule_str,
        name=schedule_name, # Use the unique name for the schedule definition
        execution_timezone=execution_timezone
    )
    def _dynamic_schedule_run_function(context): # Function name doesn't need to be globally unique if @schedule(name=...) is used
        context.log.info(f"Schedule '{context._schedule_name}' for job '{env_specific_job.name}' initiated at {context.scheduled_execution_time}.")
        # Your original schedule functions had simple logging.
        # If more complex, per-schedule logic is needed in the run function body,
        # this factory approach for the run function itself might need adjustment,
        # or you might not use a factory for the run function part.
        return {}
    
    return _dynamic_schedule_run_function


## ------------------------------------- SCHEDULES FOR DBT ASSETS ------------------------------------- ##
# these do not use the factory because they are already environment specific

# create a schedule to run stg_normalized_dbt_assets_job every day at 1130 est
@schedule(
    job=stg_normalized_dbt_assets_job,
    cron_schedule="30 23 * * *",
    execution_timezone="America/New_York"
)
def stg_normalized_dbt_assets_schedule(_):
    return {}

# create a schedule to run stg_latest_dbt_assets_job every day at 1030 est
@schedule(
    job=stg_latest_dbt_assets_job,
    cron_schedule="30 22 * * *",
    execution_timezone="America/New_York"
)
def stg_latest_dbt_assets_schedule(_):
    return {}

# create a schedule to run stg_period_change_data_dbt_assets_job every day at midnight est
@schedule(
    job=stg_period_change_data_dbt_assets_job,
    cron_schedule="0 0 * * *",
    execution_timezone="America/New_York"
)
def stg_period_change_data_dbt_assets_schedule(_):
    return {}

# create a schedule to run prod_normalized_dbt_assets_job every day at 1130 est
@schedule(
    job=prod_normalized_dbt_assets_job,
    cron_schedule="30 23 * * *",
    execution_timezone="America/New_York"
)
def prod_normalized_dbt_assets_schedule(_):
    return {}

# create a schedule to run prod_latest_dbt_assets_job every day at 1030 est
@schedule(
    job=prod_latest_dbt_assets_job,
    cron_schedule="30 22 * * *",
    execution_timezone="America/New_York"
)
def prod_latest_dbt_assets_schedule(_):
    return {}

# create a schedule to run prod_period_change_data_dbt_assets_job every day at midnight est
@schedule(
    job=prod_period_change_data_dbt_assets_job,
    cron_schedule="0 0 * * *",
    execution_timezone="America/New_York"
)
def prod_period_change_data_dbt_assets_schedule(_):
    return {}

## --------------------------- special jobs that will not get schedule created by factory
# create a schedule to run update_crypto_ecosystems_raw_file_job every wednesday at 9 pm est
@schedule(
    job=update_crypto_ecosystems_raw_file_job,
    cron_schedule="0 21 * * 3",
    execution_timezone="America/New_York"
)
def update_crypto_ecosystems_raw_file_schedule(_):
    return {}

# create a schedule to run refresh_prod_schema every night at midnight
@schedule(
    job=refresh_prod_schema,
    cron_schedule="0 0 * * *",
    execution_timezone="America/New_York"
)
def refresh_prod_schema_schedule(_):
    return {}

# create a schedule to run refresh_api_schema every night at 1 am est
# note, refreshing the api schema is not required but we do it to ensure any newly added api models are executed and no tests are failing
@schedule(
    job=refresh_api_schema,
    cron_schedule="0 1 * * *",
    execution_timezone="America/New_York"
)
def refresh_api_schema_schedule(_):
    return {}

## ------------------------------------- SCHEDULES FOR ML PIPELINE ------------------------------------- ##
# prod
@schedule(
    job=prod_ml_pipeline_job,
    cron_schedule="0 0 28 * *",  # At 12:00 AM on day 28 of the month
    execution_timezone="America/New_York"
)
def prod_ml_pipeline_schedule(_):
    return {}

# stg
@schedule(
    job=stg_ml_pipeline_job,
    cron_schedule="0 0 28 * *",  # At 12:00 AM on day 28 of the month
    execution_timezone="America/New_York"
)
def stg_ml_pipeline_schedule(_):
    return {}