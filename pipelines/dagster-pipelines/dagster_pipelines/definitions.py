import os

from dagster import Definitions
from dagster_pipelines.resources import cloud_sql_postgres_resource, dbt_resource, electric_capital_ecosystems_repo

from dagster_pipelines.assets import (
    github_project_orgs, 
    latest_active_distinct_github_project_repos,
    github_project_repos_stargaze_count, 
    github_project_repos_fork_count, 
    github_project_repos_contributors, 
    github_project_repos_languages,
    github_project_repos_commits,
    github_project_repos_watcher_count,
    github_project_repos_is_fork,
    crypto_ecosystems_project_json,
    latest_contributor_data,
    contributor_follower_counts,
    latest_contributor_following_count,
    latest_contributor_activity
)
from dagster_pipelines.cleaning_assets import ( 
    all_dbt_assets, 
    process_compressed_contributors_data,
    update_crypto_ecosystems_raw_file_job
)
from dagster_pipelines.jobs import (
    github_project_orgs_job, 
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
    update_crypto_ecosystems_repo_and_run_export_job,
    crypto_ecosystems_project_json_job,
    latest_contributor_data_job,
    contributor_follower_counts_job,
    latest_contributor_following_count_job,
    latest_contributor_activity_job
)
from dagster_pipelines.schedules import (
    github_project_orgs_schedule, 
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
    process_compressed_contributors_data_schedule,
    update_crypto_ecosystems_repo_and_run_export_schedule,
    crypto_ecosystems_project_json_schedule,
    update_crypto_ecosystems_raw_file_schedule,
    latest_contributor_data_schedule,
    contributor_follower_counts_schedule,
    latest_contributor_following_count_schedule,
    latest_contributor_activity_schedule
)
from dagster_pipelines.load_data_jobs import (
    refresh_prod_schema, 
    update_crypto_ecosystems_repo_and_run_export
)
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
        "dbt_resource": dbt_resource,
        "electric_capital_ecosystems_repo": electric_capital_ecosystems_repo
    },
    assets=[
        github_project_orgs, 
        latest_active_distinct_github_project_repos, 
        github_project_repos_stargaze_count, 
        github_project_repos_fork_count,
        github_project_repos_contributors, 
        all_dbt_assets, 
        process_compressed_contributors_data, 
        github_project_repos_languages,
        github_project_repos_commits,
        github_project_repos_watcher_count,
        github_project_repos_is_fork,
        update_crypto_ecosystems_repo_and_run_export,
        crypto_ecosystems_project_json,
        latest_contributor_data,
        contributor_follower_counts,
        latest_contributor_following_count,
        latest_contributor_activity
        ],
    jobs=[
        github_project_orgs_job, 
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
        process_compressed_contributors_data_job,
        update_crypto_ecosystems_repo_and_run_export_job,
        crypto_ecosystems_project_json_job,
        update_crypto_ecosystems_raw_file_job,
        latest_contributor_data_job,
        contributor_follower_counts_job,
        latest_contributor_following_count_job,
        latest_contributor_activity_job
        ],
    schedules=[
        github_project_orgs_schedule, 
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
        process_compressed_contributors_data_schedule,
        update_crypto_ecosystems_repo_and_run_export_schedule,
        crypto_ecosystems_project_json_schedule,
        update_crypto_ecosystems_raw_file_schedule,
        latest_contributor_data_schedule,
        contributor_follower_counts_schedule,
        latest_contributor_following_count_schedule,
        latest_contributor_activity_schedule
        ],
)
