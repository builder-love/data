import os

from dagster import Definitions, resource
from dagster_pipelines.assets import crypto_ecosystems_project_toml_files, github_project_orgs, github_project_sub_ecosystems, github_project_repos
from dagster_pipelines.latest_assets import latest_project_toml_files, latest_github_project_orgs, latest_github_project_sub_ecosystems, latest_github_project_repos
from dagster_pipelines.cleaning_assets import latest_distinct_github_project_repos, latest_active_distinct_github_project_repos
from dagster_pipelines.jobs import crypto_ecosystems_project_toml_files_job, github_project_orgs_job, github_project_sub_ecosystems_job, github_project_repos_job, latest_active_distinct_project_repos_job
from dagster_pipelines.schedules import crypto_ecosystems_project_toml_files_schedule, github_project_orgs_schedule, github_project_sub_ecosystems_schedule, github_project_repos_schedule, latest_active_distinct_project_repos_schedule
from sqlalchemy import create_engine

# define the cloud sql postgres resource
@resource(
    config_schema={
        "username": str,
        "password": str,
        "hostname": str,
        "database": str,
    }
)
def cloud_sql_postgres_resource(context):
    # Construct the connection string
    conn_str = (
        f"postgresql+psycopg2://"
        f"{context.resource_config['username']}:{context.resource_config['password']}@"
        f"{context.resource_config['hostname']}/{context.resource_config['database']}"
    )

    # Create the engine
    engine = create_engine(conn_str)
    return engine

# Include the resource and assets and define a job
defs = Definitions(
    assets=[crypto_ecosystems_project_toml_files, latest_project_toml_files, github_project_orgs, latest_github_project_orgs, 
            github_project_sub_ecosystems, latest_github_project_sub_ecosystems, github_project_repos, latest_github_project_repos, 
            latest_distinct_github_project_repos, latest_active_distinct_github_project_repos],
    resources={
        "cloud_sql_postgres_resource": cloud_sql_postgres_resource.configured(
            {
                "username": os.getenv("cloud_sql_user"),
                "password": os.getenv("cloud_sql_password"),
                "hostname": os.getenv("cloud_sql_postgres_host"),
                "database": os.getenv("cloud_sql_postgres_db"),
            }
        )
    },
    jobs=[crypto_ecosystems_project_toml_files_job, github_project_orgs_job, github_project_sub_ecosystems_job, github_project_repos_job,
          latest_active_distinct_project_repos_job],
    schedules=[crypto_ecosystems_project_toml_files_schedule, github_project_orgs_schedule, github_project_sub_ecosystems_schedule, 
               github_project_repos_schedule, latest_active_distinct_project_repos_schedule],
)
