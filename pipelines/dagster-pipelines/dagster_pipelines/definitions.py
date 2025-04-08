import os

from dagster import Definitions
from dagster_pipelines.resources import cloud_sql_postgres_resource, dbt_resource

from dagster_pipelines.assets import (
    crypto_ecosystems_project_toml_files, 
    github_project_orgs, 
    github_project_sub_ecosystems, 
    github_project_repos, 
    latest_active_distinct_github_project_repos,
    github_project_repos_stargaze_count, 
    github_project_repos_fork_count, 
    github_project_repos_contributors, 
    github_project_repos_languages,
    github_project_repos_commits,
    github_project_repos_watcher_count,
    github_project_repos_is_fork
)
from dagster_pipelines.cleaning_assets import ( 
    all_dbt_assets, 
    process_compressed_contributors_data
)
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
    process_compressed_contributors_data_job,
    period_change_data_dbt_assets_job,
    project_repos_watcher_count_job,
    project_repos_is_fork_job,
    process_compressed_contributors_data_job
)
from dagster_pipelines.schedules import (
    crypto_ecosystems_project_toml_files_schedule, 
    github_project_orgs_schedule, 
    github_project_sub_ecosystems_schedule, 
    github_project_repos_schedule, 
    latest_active_distinct_project_repos_schedule, 
    project_repos_stargaze_count_schedule, 
    project_repos_fork_count_schedule, 
    normalized_dbt_assets_schedule, 
    project_repos_languages_schedule,
    latest_dbt_assets_schedule,
    project_repos_commit_count_schedule,
    refresh_prod_schema_schedule,
    refresh_api_schema_schedule,
    project_repos_contributors_schedule,
    period_change_data_dbt_assets_schedule,
    project_repos_watcher_count_schedule,
    project_repos_is_fork_schedule,
    process_compressed_contributors_data_schedule
)
from dagster_pipelines.load_data_jobs import refresh_prod_schema
from dagster_pipelines.api_data import refresh_api_schema

# Include the resource and assets and define a job
defs = Definitions(
    resources={
        "cloud_sql_postgres_resource": cloud_sql_postgres_resource.configured(
            {
                "username": os.getenv("cloud_sql_user"),
                "password": os.getenv("cloud_sql_password"),
                "hostname": os.getenv("cloud_sql_postgres_host"),
                "database": os.getenv("cloud_sql_postgres_db"),
            }
        ),
        "dbt_resource": dbt_resource
    },
    assets=[
        crypto_ecosystems_project_toml_files, 
        github_project_orgs, 
        github_project_sub_ecosystems, 
        github_project_repos, 
        latest_active_distinct_github_project_repos, 
        github_project_repos_stargaze_count, 
        github_project_repos_fork_count,
        github_project_repos_contributors, 
        all_dbt_assets, 
        process_compressed_contributors_data, 
        github_project_repos_languages,
        github_project_repos_commits,
        github_project_repos_watcher_count,
        github_project_repos_is_fork
        ],
    jobs=[
        crypto_ecosystems_project_toml_files_job, 
        github_project_orgs_job, 
        github_project_sub_ecosystems_job, 
        github_project_repos_job,
        latest_active_distinct_project_repos_job, 
        project_repos_stargaze_count_job, 
        project_repos_fork_count_job, 
        normalized_dbt_assets_job,
        project_repos_languages_job, 
        refresh_prod_schema, 
        latest_dbt_assets_job,
        project_repos_commit_count_job,
        project_repos_contributors_job,
        refresh_api_schema,
        period_change_data_dbt_assets_job,
        project_repos_watcher_count_job,
        project_repos_is_fork_job,
        process_compressed_contributors_data_job
        ],
    schedules=[
        crypto_ecosystems_project_toml_files_schedule, 
        github_project_orgs_schedule, 
        github_project_sub_ecosystems_schedule, 
        github_project_repos_schedule, 
        latest_active_distinct_project_repos_schedule, 
        project_repos_stargaze_count_schedule, 
        project_repos_fork_count_schedule,
        normalized_dbt_assets_schedule, 
        project_repos_languages_schedule,
        latest_dbt_assets_schedule,
        project_repos_commit_count_schedule,
        refresh_prod_schema_schedule,
        refresh_api_schema_schedule,
        project_repos_contributors_schedule,
        period_change_data_dbt_assets_schedule,
        project_repos_watcher_count_schedule,
        project_repos_is_fork_schedule,
        process_compressed_contributors_data_schedule
        ],
)
