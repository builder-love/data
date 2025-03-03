from dagster import job
import dagster as dg
from dagster_pipelines.assets import crypto_ecosystems_project_toml_files, github_project_orgs, github_project_sub_ecosystems, github_project_repos
from dagster_pipelines.cleaning_assets import latest_active_distinct_github_project_repos

# create a job to run crypto_ecosystems_project_toml_files asset
crypto_ecosystems_project_toml_files_job = dg.define_asset_job(
    "project_toml_files_refresh", 
    selection=["crypto_ecosystems_project_toml_files"],
    tags={"github_api": "True"}
)

# create a job to run github_project_orgs asset
github_project_orgs_job = dg.define_asset_job(
    "github_project_orgs_refresh", 
    selection=["github_project_orgs"],
    tags={"github_api": "True"}
)

# create a job to run github_project_sub_ecosystems asset
github_project_sub_ecosystems_job = dg.define_asset_job(
    "github_project_sub_ecosystems_refresh", 
    selection=["github_project_sub_ecosystems"],
    tags={"github_api": "True"}
)

# create a job to run github_project_repos asset
github_project_repos_job = dg.define_asset_job(
    "github_project_repos_refresh", 
    selection=["github_project_repos"],
    tags={"github_api": "True"}
)

# create a job to run latest_active_distinct_project_repos asset
latest_active_distinct_project_repos_job = dg.define_asset_job(
    "latest_active_distinct_project_repos_refresh", 
    selection=["latest_active_distinct_github_project_repos"],
    tags={"github_api": "True"}
)