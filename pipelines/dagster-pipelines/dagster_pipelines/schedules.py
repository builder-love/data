from dagster import schedule, ScheduleDefinition, define_asset_job, JobDefinition
from dagster_dbt import build_schedule_from_dbt_selection
from dagster_pipelines.jobs import (
    stg_normalized_dbt_assets_job,
    stg_latest_dbt_assets_job,
    stg_period_change_data_dbt_assets_job,
    prod_normalized_dbt_assets_job,
    prod_latest_dbt_assets_job,
    prod_period_change_data_dbt_assets_job,
    update_crypto_ecosystems_raw_file_job
)
from dagster_pipelines.cleaning_assets import all_stg_dbt_assets, all_prod_dbt_assets
from dagster_pipelines.load_data_jobs import refresh_prod_schema
from dagster_pipelines.api_data import refresh_api_schema


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
# Create the schedule
stg_normalized_dbt_assets_schedule = ScheduleDefinition(
    job=stg_normalized_dbt_assets_job,  # The job to run
    cron_schedule="30 23 * * *",
    execution_timezone="America/New_York", 
    name="stg_normalized_dbt_assets_daily_schedule",  # Give the schedule a name
)

# create a schedule to run stg_latest_dbt_assets_job every day at 1030 est
# Create the schedule
stg_latest_dbt_assets_schedule = ScheduleDefinition(
    job=stg_latest_dbt_assets_job,  # The job to run
    cron_schedule="30 22 * * *",
    execution_timezone="America/New_York", 
    name="stg_latest_dbt_assets_daily_schedule",  # Give the schedule a name
)

# create a schedule to run stg_period_change_data_dbt_assets_job every day at midnight est
# Create the schedule
stg_period_change_data_dbt_assets_schedule = ScheduleDefinition(
    job=stg_period_change_data_dbt_assets_job,  # The job to run
    cron_schedule="0 0 * * *",
    execution_timezone="America/New_York", 
    name="stg_period_change_data_dbt_assets_daily_schedule",  # Give the schedule a name
)

# create a schedule to run prod_normalized_dbt_assets_job every day at 1130 est
# Create the schedule
prod_normalized_dbt_assets_schedule = ScheduleDefinition(
    job=prod_normalized_dbt_assets_job,  # The job to run
    cron_schedule="30 23 * * *",
    execution_timezone="America/New_York", 
    name="prod_normalized_dbt_assets_daily_schedule",  # Give the schedule a name
)

# create a schedule to run prod_latest_dbt_assets_job every day at 1030 est
# Create the schedule
prod_latest_dbt_assets_schedule = ScheduleDefinition(
    job=prod_latest_dbt_assets_job,  # The job to run
    cron_schedule="30 22 * * *",
    execution_timezone="America/New_York", 
    name="prod_latest_dbt_assets_daily_schedule",  # Give the schedule a name
)

# create a schedule to run prod_period_change_data_dbt_assets_job every day at midnight est
# Create the schedule
prod_period_change_data_dbt_assets_schedule = ScheduleDefinition(
    job=prod_period_change_data_dbt_assets_job,  # The job to run
    cron_schedule="0 0 * * *",
    execution_timezone="America/New_York", 
    name="prod_period_change_data_dbt_assets_daily_schedule",  # Give the schedule a name
)

## --------------------------- special jobs that will not get schedule created by factory
# create a schedule to run update_crypto_ecosystems_raw_file_job every wednesday at 9 pm est
@schedule(
    job=update_crypto_ecosystems_raw_file_job,
    cron_schedule="0 21 * * 3",
    execution_timezone="America/New_York"
)
def update_crypto_ecosystems_raw_file_schedule(context):
    # Log the start time of the job
    context.log.info(f"update_crypto_ecosystems_raw_file_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"update_crypto_ecosystems_raw_file_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"update_crypto_ecosystems_raw_file_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run refresh_prod_schema every night at midnight
@schedule(
    job=refresh_prod_schema,
    cron_schedule="0 0 * * *",
    execution_timezone="America/New_York"
)
def refresh_prod_schema_schedule(context):
    # Log the start time of the job
    context.log.info(f"refresh_prod_schema_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"refresh_prod_schema_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time

    # Log the duration of the job
    context.log.info(f"refresh_prod_schema_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run refresh_api_schema every night at 1 am est
# note, refreshing the api schema is not required but we do it to ensure any newly added api models are executed and no tests are failing
@schedule(
    job=refresh_api_schema,
    cron_schedule="0 1 * * *",
    execution_timezone="America/New_York"
)
def refresh_api_schema_schedule(context):
    # Log the start time of the job
    context.log.info(f"refresh_api_schema_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"refresh_api_schema_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time

    # Log the duration of the job
    context.log.info(f"refresh_api_schema_schedule job duration: {end_time - start_time}")

    return {}

## ------------------------------------- old method of creating schedules ------------------------------------- ##
# replaced with factory above

# # create a schedule to run update_repo_and_run_export_job on wednesday at 7 pm est
# @schedule(
#     job=update_crypto_ecosystems_repo_and_run_export_job,
#     cron_schedule="0 19 * * 3", 
#     execution_timezone="America/New_York"
# )
# def update_crypto_ecosystems_repo_and_run_export_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"update_crypto_ecosystems_repo_and_run_export_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"update_crypto_ecosystems_repo_and_run_export_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"update_crypto_ecosystems_repo_and_run_export_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run crypto_ecosystems_project_json_job every Wednesday at 8 PM est
# @schedule(
#     job=crypto_ecosystems_project_json_job,
#     cron_schedule="0 20 * * 3", 
#     execution_timezone="America/New_York"
# )
# def crypto_ecosystems_project_json_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"crypto_ecosystems_project_json_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"crypto_ecosystems_project_json_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"crypto_ecosystems_project_json_schedule job duration: {end_time - start_time}")

#     return {}


# # create a schedule to run latest_active_distinct_project_repos_job on the 7th and 21st of each month at 10 minutes past midnight
# @schedule(
#     job=latest_active_distinct_project_repos_job,
#     cron_schedule="10 0 7,21 * *", 
#     execution_timezone="America/New_York"
# )
# def latest_active_distinct_project_repos_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"latest_active_distinct_project_repos_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"latest_active_distinct_project_repos_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"latest_active_distinct_project_repos_schedule job duration: {end_time - start_time}")

#     return {}


# # create a schedule to run project_repos_stargaze_count_job every 7 days at midnight
# @schedule(
#     job=project_repos_stargaze_count_job,
#     cron_schedule="10 0 * * 0", 
#     execution_timezone="America/New_York"
# )
# def project_repos_stargaze_count_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"project_repos_stargaze_count_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"project_repos_stargaze_count_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"project_repos_stargaze_count_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run project_repos_fork_count_job every saturday at midnight
# @schedule(
#     job=project_repos_fork_count_job,
#     cron_schedule="10 0 * * 6", 
#     execution_timezone="America/New_York"
# )
# def project_repos_fork_count_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"project_repos_fork_count_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"project_repos_fork_count_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"project_repos_fork_count_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run project_repos_languages_job on the 16th of each month at midnight
# @schedule(
#     job=project_repos_languages_job,
#     cron_schedule="0 0 16 * *", 
#     execution_timezone="America/New_York"
# )
# def project_repos_languages_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"project_repos_languages_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"project_repos_languages_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"project_repos_languages_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run project_repos_commit_count_job on the 16th of each month at midnight
# @schedule(
#     job=project_repos_commit_count_job,
#     cron_schedule="10 0 * * 5", 
#     execution_timezone="America/New_York"
# )
# def project_repos_commit_count_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"project_repos_commit_count_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"project_repos_commit_count_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"project_repos_commit_count_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run project_repos_watcher_count_job every 7 days at midnight
# @schedule(
#     job=project_repos_watcher_count_job,
#     cron_schedule="10 0 * * 1", 
#     execution_timezone="America/New_York"
# )
# def project_repos_watcher_count_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"project_repos_watcher_count_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"project_repos_watcher_count_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"project_repos_watcher_count_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run project_repos_contributors_job on the 16th of each month at midnight
# @schedule(
#     job=project_repos_contributors_job,
#     cron_schedule="0 0 20 * *", 
#     execution_timezone="America/New_York"
# )
# def project_repos_contributors_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"project_repos_contributors_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"project_repos_contributors_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"project_repos_contributors_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run process_compressed_contributors_data_job every 7 days at midnight
# @schedule(
#     job=process_compressed_contributors_data_job,
#     cron_schedule="0 3 * * *", 
#     execution_timezone="America/New_York"
# )
# def process_compressed_contributors_data_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"process_compressed_contributors_data_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"process_compressed_contributors_data_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"process_compressed_contributors_data_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run latest_contributor_data_job on the 8th and 22nd of each month at 10 minutes past midnight
# @schedule(
#     job=latest_contributor_data_job,
#     cron_schedule="0 10 8,22 * *",
#     execution_timezone="America/New_York"
# )
# def latest_contributor_data_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"latest_contributor_data_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"latest_contributor_data_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"latest_contributor_data_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run contributor_follower_counts_job on the 9th of each month at 10 minutes past midnight
# @schedule(
#     job=contributor_follower_counts_job,
#     cron_schedule="10 0 9 * *",
#     execution_timezone="America/New_York"
# )
# def contributor_follower_counts_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"contributor_follower_counts_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"contributor_follower_counts_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"contributor_follower_counts_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run latest_contributor_following_count_job on the 12th of each month at 10 minutes past midnight
# @schedule(
#     job=latest_contributor_following_count_job,
#     cron_schedule="10 0 12 * *",
#     execution_timezone="America/New_York"
# )
# def latest_contributor_following_count_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"latest_contributor_following_count_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"latest_contributor_following_count_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"latest_contributor_following_count_schedule job duration: {end_time - start_time}")

#     return {}

# # create a schedule to run latest_contributor_activity_job on the 13th of each month at 10 minutes past midnight
# @schedule(
#     job=latest_contributor_activity_job,
#     cron_schedule="10 0 13 * *",
#     execution_timezone="America/New_York"
# )
# def latest_contributor_activity_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"latest_contributor_activity_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"latest_contributor_activity_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"latest_contributor_activity_schedule job duration: {end_time - start_time}")

#     return {}


# # create a schedule to run project_repos_is_fork_job every 7 days at midnight
# @schedule(
#     job=project_repos_is_fork_job,
#     cron_schedule="10 0 * * 2", 
#     execution_timezone="America/New_York"
# )
# def project_repos_is_fork_schedule(context):
#     # Log the start time of the job
#     context.log.info(f"project_repos_is_fork_schedule job started at: {context.scheduled_execution_time}")
#     start_time = context.scheduled_execution_time

#     # Log the end time of the job
#     context.log.info(f"project_repos_is_fork_schedule job ended at: {context.scheduled_execution_time}")
#     end_time = context.scheduled_execution_time
    
#     # Log the duration of the job
#     context.log.info(f"project_repos_is_fork_schedule job duration: {end_time - start_time}")

#     return {}