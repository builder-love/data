from dagster import schedule
from dagster_pipelines.jobs import crypto_ecosystems_project_toml_files_job, github_project_orgs_job, github_project_sub_ecosystems_job, github_project_repos_job, latest_active_distinct_project_repos_job

# create a schedule to run crypto_ecosystems_project_toml_files_job every 15 days at midnight
@schedule(
    job=crypto_ecosystems_project_toml_files_job,
    cron_schedule="0 3 15 * *"
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
    cron_schedule="10 3 15 * *"
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
    cron_schedule="20 3 15 * *"
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
    cron_schedule="30 3 15 * *"
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
    cron_schedule="40 3 15 * *"
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