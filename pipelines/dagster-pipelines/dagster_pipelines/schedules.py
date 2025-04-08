from dagster import schedule, ScheduleDefinition, define_asset_job
from dagster_dbt import build_schedule_from_dbt_selection
from dagster_pipelines.jobs import (
    crypto_ecosystems_project_toml_files_job, 
    github_project_orgs_job, 
    github_project_sub_ecosystems_job, 
    github_project_repos_job, 
    latest_active_distinct_project_repos_job, 
    project_repos_stargaze_count_job, 
    project_repos_fork_count_job, 
    normalized_dbt_assets_job, 
    project_repos_languages_job,
    latest_dbt_assets_job,
    project_repos_commit_count_job,
    project_repos_contributors_job,
    period_change_data_dbt_assets_job,
    project_repos_watcher_count_job,
    project_repos_is_fork_job
)
from dagster_pipelines.cleaning_assets import all_dbt_assets
from dagster_pipelines.load_data_jobs import refresh_prod_schema
from dagster_pipelines.api_data import refresh_api_schema

# create a schedule to run crypto_ecosystems_project_toml_files_job every 15 days at midnight
@schedule(
    job=crypto_ecosystems_project_toml_files_job,
    cron_schedule="0 0 15 * *", 
    execution_timezone="America/New_York"
)
def crypto_ecosystems_project_toml_files_schedule(context):
    # Log the start time of the job
    context.log.info(f"crypto_ecosystems_project_toml_files_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"crypto_ecosystems_project_toml_files_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"crypto_ecosystems_project_toml_files_schedule job duration: {end_time - start_time}")

    return {}


# create a schedule to run github_project_orgs_job every 15 days at midnight
@schedule(
    job=github_project_orgs_job,
    cron_schedule="10 0 15 * *", 
    execution_timezone="America/New_York"
)
def github_project_orgs_schedule(context):
    # Log the start time of the job
    context.log.info(f"github_project_orgs_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"github_project_orgs_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"github_project_orgs_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run github_project_sub_ecosystems_job every 15 days at midnight
@schedule(
    job=github_project_sub_ecosystems_job,
    cron_schedule="20 0 15 * *", 
    execution_timezone="America/New_York"
)
def github_project_sub_ecosystems_schedule(context):
    # Log the start time of the job
    context.log.info(f"github_project_sub_ecosystems_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"github_project_sub_ecosystems_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"github_project_sub_ecosystems_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run github_project_repos_job every 15 days at midnight
@schedule(
    job=github_project_repos_job,
    cron_schedule="30 0 15 * *", 
    execution_timezone="America/New_York"
)
def github_project_repos_schedule(context):
    # Log the start time of the job
    context.log.info(f"github_project_repos_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"github_project_repos_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"github_project_repos_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run latest_active_distinct_project_repos_job every 15 days at midnight
@schedule(
    job=latest_active_distinct_project_repos_job,
    cron_schedule="10 0 7,21 * *", 
    execution_timezone="America/New_York"
)
def latest_active_distinct_project_repos_schedule(context):
    # Log the start time of the job
    context.log.info(f"latest_active_distinct_project_repos_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"latest_active_distinct_project_repos_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"latest_active_distinct_project_repos_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run project_repos_stargaze_count_job every 7 days at midnight
@schedule(
    job=project_repos_stargaze_count_job,
    cron_schedule="10 0 * * 0", 
    execution_timezone="America/New_York"
)
def project_repos_stargaze_count_schedule(context):
    # Log the start time of the job
    context.log.info(f"project_repos_stargaze_count_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"project_repos_stargaze_count_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"project_repos_stargaze_count_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run project_repos_fork_count_job every saturday at midnight
@schedule(
    job=project_repos_fork_count_job,
    cron_schedule="10 0 * * 6", 
    execution_timezone="America/New_York"
)
def project_repos_fork_count_schedule(context):
    # Log the start time of the job
    context.log.info(f"project_repos_fork_count_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"project_repos_fork_count_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"project_repos_fork_count_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run project_repos_languages_job on the 16th of each month at midnight
@schedule(
    job=project_repos_languages_job,
    cron_schedule="0 0 16 * *", 
    execution_timezone="America/New_York"
)
def project_repos_languages_schedule(context):
    # Log the start time of the job
    context.log.info(f"project_repos_languages_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"project_repos_languages_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"project_repos_languages_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run project_repos_commit_count_job on the 16th of each month at midnight
@schedule(
    job=project_repos_commit_count_job,
    cron_schedule="10 0 * * 5", 
    execution_timezone="America/New_York"
)
def project_repos_commit_count_schedule(context):
    # Log the start time of the job
    context.log.info(f"project_repos_commit_count_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"project_repos_commit_count_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"project_repos_commit_count_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run project_repos_watcher_count_job every 7 days at midnight
@schedule(
    job=project_repos_watcher_count_job,
    cron_schedule="10 0 * * 1", 
    execution_timezone="America/New_York"
)
def project_repos_watcher_count_schedule(context):
    # Log the start time of the job
    context.log.info(f"project_repos_watcher_count_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"project_repos_watcher_count_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"project_repos_watcher_count_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run project_repos_contributors_job on the 16th of each month at midnight
@schedule(
    job=project_repos_contributors_job,
    cron_schedule="0 0 20 * *", 
    execution_timezone="America/New_York"
)
def project_repos_contributors_schedule(context):
    # Log the start time of the job
    context.log.info(f"project_repos_contributors_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"project_repos_contributors_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"project_repos_contributors_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run project_repos_is_fork_job every 7 days at midnight
@schedule(
    job=project_repos_is_fork_job,
    cron_schedule="10 0 * * 2", 
    execution_timezone="America/New_York"
)
def project_repos_is_fork_schedule(context):
    # Log the start time of the job
    context.log.info(f"project_repos_is_fork_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"project_repos_is_fork_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"project_repos_is_fork_schedule job duration: {end_time - start_time}")

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

# create a schedule to run timestamp_normalized_project_toml_files_job every day at 1130 est
# Create the schedule
normalized_dbt_assets_schedule = ScheduleDefinition(
    job=normalized_dbt_assets_job,  # The job to run
    cron_schedule="30 23 * * *",
    execution_timezone="America/New_York", 
    name="normalized_dbt_assets_daily_schedule",  # Give the schedule a name
)

# to select a specific dbt asset, use the following code
# normalized_dbt_assets_schedule = build_schedule_from_dbt_selection(
#     [all_dbt_assets],
#     job_name="normalized_dbt_assets_job",  # Give the schedule a name
#     cron_schedule="50 9 * * *",
#     execution_timezone="America/New_York", 
#     dbt_select="fqn:normalized_project_organizations"
# )

# create a schedule to run latest_dbt_assets_job every day at 1030 est
# Create the schedule
latest_dbt_assets_schedule = ScheduleDefinition(
    job=latest_dbt_assets_job,  # The job to run
    cron_schedule="30 22 * * *",
    execution_timezone="America/New_York", 
    name="latest_dbt_assets_daily_schedule",  # Give the schedule a name
)

# create a schedule to run period_change_data_dbt_assets_job every day at midnight est
# Create the schedule
period_change_data_dbt_assets_schedule = ScheduleDefinition(
    job=period_change_data_dbt_assets_job,  # The job to run
    cron_schedule="0 0 * * *",
    execution_timezone="America/New_York", 
    name="period_change_data_dbt_assets_daily_schedule",  # Give the schedule a name
)