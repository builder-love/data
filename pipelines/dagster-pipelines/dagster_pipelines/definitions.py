import os

from dagster import Definitions
from dagster_pipelines.assets import crypto_ecosystems_project_toml_files, github_project_orgs, github_project_sub_ecosystems, github_project_repos, github_project_repos_stargaze_count, github_project_repos_fork_count
from dagster_pipelines.latest_assets import latest_project_toml_files, latest_github_project_orgs, latest_github_project_sub_ecosystems, latest_github_project_repos
from dagster_pipelines.cleaning_assets import latest_distinct_github_project_repos, latest_active_distinct_github_project_repos, all_dbt_assets
from dagster_pipelines.jobs import crypto_ecosystems_project_toml_files_job, github_project_orgs_job, github_project_sub_ecosystems_job, github_project_repos_job, latest_active_distinct_project_repos_job, project_repos_stargaze_count_job, project_repos_fork_count_job, normalized_dbt_assets_job
from dagster_pipelines.schedules import crypto_ecosystems_project_toml_files_schedule, github_project_orgs_schedule, github_project_sub_ecosystems_schedule, github_project_repos_schedule, latest_active_distinct_project_repos_schedule, project_repos_stargaze_count_schedule, project_repos_fork_count_schedule, normalized_dbt_assets_schedule
from dagster_pipelines.resources import cloud_sql_postgres_resource, dbt_resource

# Include the resource and assets and define a job
defs = Definitions(
    assets=[crypto_ecosystems_project_toml_files, latest_project_toml_files, github_project_orgs, latest_github_project_orgs, 
            github_project_sub_ecosystems, latest_github_project_sub_ecosystems, github_project_repos, latest_github_project_repos, 
            latest_distinct_github_project_repos, latest_active_distinct_github_project_repos, github_project_repos_stargaze_count, github_project_repos_fork_count,
            all_dbt_assets
            ],
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
    jobs=[crypto_ecosystems_project_toml_files_job, github_project_orgs_job, github_project_sub_ecosystems_job, github_project_repos_job,
          latest_active_distinct_project_repos_job, project_repos_stargaze_count_job, project_repos_fork_count_job, normalized_dbt_assets_job
          ],
    schedules=[crypto_ecosystems_project_toml_files_schedule, github_project_orgs_schedule, github_project_sub_ecosystems_schedule, 
               github_project_repos_schedule, latest_active_distinct_project_repos_schedule, project_repos_stargaze_count_schedule, project_repos_fork_count_schedule,
               normalized_dbt_assets_schedule
               ],
)
