from dagster import schedule, ScheduleDefinition, define_asset_job
from dagster_dbt import build_schedule_from_dbt_selection
from dagster_pipelines.jobs import (
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
    project_repos_is_fork_job,
    process_compressed_contributors_data_job,
    update_crypto_ecosystems_repo_and_run_export_job,
    crypto_ecosystems_project_json_job,
    latest_contributor_data_job,
    contributor_follower_counts_job,
    latest_contributor_following_count_job,
    latest_contributor_activity_job
)
from dagster_pipelines.cleaning_assets import all_dbt_assets, update_crypto_ecosystems_raw_file_job
from dagster_pipelines.load_data_jobs import refresh_prod_schema
from dagster_pipelines.api_data import refresh_api_schema

# create a schedule to run update_repo_and_run_export_job on wednesday at 7 pm est
@schedule(
    job=update_crypto_ecosystems_repo_and_run_export_job,
    cron_schedule="0 19 * * 3", 
    execution_timezone="America/New_York"
)
def update_crypto_ecosystems_repo_and_run_export_schedule(context):
    # Log the start time of the job
    context.log.info(f"update_crypto_ecosystems_repo_and_run_export_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"update_crypto_ecosystems_repo_and_run_export_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"update_crypto_ecosystems_repo_and_run_export_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run crypto_ecosystems_project_json_job every Wednesday at 8 PM est
@schedule(
    job=crypto_ecosystems_project_json_job,
    cron_schedule="0 20 * * 3", 
    execution_timezone="America/New_York"
)
def crypto_ecosystems_project_json_schedule(context):
    # Log the start time of the job
    context.log.info(f"crypto_ecosystems_project_json_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"crypto_ecosystems_project_json_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"crypto_ecosystems_project_json_schedule job duration: {end_time - start_time}")

    return {}

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

# create a schedule to run latest_active_distinct_project_repos_job on the 7th and 21st of each month at 10 minutes past midnight
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

# create a schedule to run process_compressed_contributors_data_job every 7 days at midnight
@schedule(
    job=process_compressed_contributors_data_job,
    cron_schedule="0 3 * * *", 
    execution_timezone="America/New_York"
)
def process_compressed_contributors_data_schedule(context):
    # Log the start time of the job
    context.log.info(f"process_compressed_contributors_data_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"process_compressed_contributors_data_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"process_compressed_contributors_data_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run latest_contributor_data_job on the 8th and 22nd of each month at 10 minutes past midnight
@schedule(
    job=latest_contributor_data_job,
    cron_schedule="0 10 8,22 * *",
    execution_timezone="America/New_York"
)
def latest_contributor_data_schedule(context):
    # Log the start time of the job
    context.log.info(f"latest_contributor_data_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"latest_contributor_data_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"latest_contributor_data_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run contributor_follower_counts_job on the 9th of each month at 10 minutes past midnight
@schedule(
    job=contributor_follower_counts_job,
    cron_schedule="10 0 9 * *",
    execution_timezone="America/New_York"
)
def contributor_follower_counts_schedule(context):
    # Log the start time of the job
    context.log.info(f"contributor_follower_counts_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"contributor_follower_counts_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"contributor_follower_counts_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run latest_contributor_following_count_job on the 12th of each month at 10 minutes past midnight
@schedule(
    job=latest_contributor_following_count_job,
    cron_schedule="10 0 12 * *",
    execution_timezone="America/New_York"
)
def latest_contributor_following_count_schedule(context):
    # Log the start time of the job
    context.log.info(f"latest_contributor_following_count_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"latest_contributor_following_count_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"latest_contributor_following_count_schedule job duration: {end_time - start_time}")

    return {}

# create a schedule to run latest_contributor_activity_job on the 13th of each month at 10 minutes past midnight
@schedule(
    job=latest_contributor_activity_job,
    cron_schedule="10 0 13 * *",
    execution_timezone="America/New_York"
)
def latest_contributor_activity_schedule(context):
    # Log the start time of the job
    context.log.info(f"latest_contributor_activity_schedule job started at: {context.scheduled_execution_time}")
    start_time = context.scheduled_execution_time

    # Log the end time of the job
    context.log.info(f"latest_contributor_activity_schedule job ended at: {context.scheduled_execution_time}")
    end_time = context.scheduled_execution_time
    
    # Log the duration of the job
    context.log.info(f"latest_contributor_activity_schedule job duration: {end_time - start_time}")

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

# create a schedule to run timestamp_normalized job every day at 1130 est
# Create the schedule
normalized_dbt_assets_schedule = ScheduleDefinition(
    job=normalized_dbt_assets_job,  # The job to run
    cron_schedule="30 23 * * *",
    execution_timezone="America/New_York", 
    name="normalized_dbt_assets_daily_schedule",  # Give the schedule a name
)

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