from dagster import job
import dagster as dg
from dagster import define_asset_job
from dagster_pipelines.assets import (
    crypto_ecosystems_project_toml_files, 
    github_project_orgs, 
    github_project_sub_ecosystems, 
    github_project_repos, 
    latest_active_distinct_github_project_repos,
    github_project_repos_stargaze_count, 
    github_project_repos_fork_count, 
    github_project_repos_languages,
    github_project_repos_commits,
    github_project_repos_contributors,
    github_project_repos_watcher_count,
    github_project_repos_is_fork
)
from dagster_pipelines.cleaning_assets import (
    process_compressed_contributors_data
)

# create a job to run crypto_ecosystems_project_toml_files asset
crypto_ecosystems_project_toml_files_job = dg.define_asset_job(
    "project_toml_files_refresh", 
    selection=["crypto_ecosystems_project_toml_files"],
    tags={"github_api": "True"},
    description="Appends toml files from the crypto_ecosystems repo to the raw.project_toml_files table"
)

# create a job to run github_project_orgs asset
github_project_orgs_job = dg.define_asset_job(
    "github_project_orgs_refresh", 
    selection=["github_project_orgs"],
    tags={"github_api": "True"},
    description="Gets the listed orgs from the project's toml file in the crypto_ecosystems repo and appends them to the raw.github_project_organizations table"
)

# create a job to run github_project_sub_ecosystems asset
github_project_sub_ecosystems_job = dg.define_asset_job(
    "github_project_sub_ecosystems_refresh", 
    selection=["github_project_sub_ecosystems"],
    tags={"github_api": "True"},
    description="Gets the listed sub ecosystems from the project's toml file in the crypto_ecosystems repo and appends them to the raw.github_project_sub_ecosystems table"
)

# create a job to run github_project_repos asset
github_project_repos_job = dg.define_asset_job(
    "github_project_repos_refresh", 
    selection=["github_project_repos"],
    tags={"github_api": "True"},
    description="Gets the listed repos from the project's toml file in the crypto_ecosystems repo and appends them to the raw.github_project_repos table"
)

# create a job to run latest_active_distinct_project_repos asset
latest_active_distinct_project_repos_job = dg.define_asset_job(
    "latest_active_distinct_project_repos_refresh", 
    selection=["latest_active_distinct_github_project_repos"],
    tags={"github_api": "True"},
    description="Queries the latest distinct list of repos to check if the repo is still active and public. Overwrites the data in the raw.latest_active_distinct_github_project_repos table"
)

# create a job to run github_project_repos_stargaze_count asset
project_repos_stargaze_count_job = dg.define_asset_job(
    "project_repos_stargaze_count_refresh", 
    selection=["github_project_repos_stargaze_count"],
    tags={"github_api": "True"},
    description="Gets the stargaze count for each repo from the github api and appends it to the raw.github_project_repos_stargaze_count table"
)

# create a job to run github_project_repos_fork_count asset
project_repos_fork_count_job = dg.define_asset_job(
    "project_repos_fork_count_refresh", 
    selection=["github_project_repos_fork_count"],
    tags={"github_api": "True"},
    description="Gets the fork count for each repo from the github api and appends it to the raw.github_project_repos_fork_count table"
)

# create a job to run github_project_repos_languages asset
project_repos_languages_job = dg.define_asset_job(
    "project_repos_languages_refresh", 
    selection=["github_project_repos_languages"],
    tags={"github_api": "True"},
    description="Gets the languages for each repo from the github api and appends it to the raw.github_project_repos_languages table"
)

# create a job to run github_project_repos_commits asset
project_repos_commit_count_job = dg.define_asset_job(
    "project_repos_commit_count_refresh", 
    selection=["github_project_repos_commits"],
    tags={"github_api": "True"},
    description="Gets the commit count for each repo from the github api and appends it to the raw.github_project_repos_commits table"
)

# create a job to run github_project_repos_watchers asset
project_repos_watcher_count_job = dg.define_asset_job(
    "project_repos_watcher_count_refresh", 
    selection=["github_project_repos_watcher_count"],
    tags={"github_api": "True"},
    description="Gets the watcher count for each repo from the github api and appends it to the raw.project_repos_watcher_count table"
)

# create a job to run github_project_repos_is_fork asset
project_repos_is_fork_job = dg.define_asset_job(
    "project_repos_is_fork_refresh", 
    selection=["github_project_repos_is_fork"],
    tags={"github_api": "True"},
    description="Gets the is_fork for each repo from the github api and appends it to the raw.project_repos_is_fork table"
)
# create a job to run github_project_repos_contributors asset
project_repos_contributors_job = dg.define_asset_job(
    "project_repos_contributors_refresh", 
    selection=["github_project_repos_contributors"],
    tags={"github_api": "True"},
    description="Gets the contributors for each repo from the github api, compresses the data and appends it to the raw.github_project_repos_contributors table"
)

# create a job to run process_compressed_contributors_data asset
process_compressed_contributors_data_job = dg.define_asset_job(
    "process_compressed_contributors_data_refresh", 
    selection=["process_compressed_contributors_data"],
    tags={"github_api": "True"},
    description="Extracts, decompresses, and inserts data into the clean table. Only for the latest data in the raw.project_repos_contributors table. Performs checks for outliers before refreshing clean table."
)

# Define a Dagster job that uses the dbt assets
# run tag:normalized dbt assets
normalized_dbt_assets_job = define_asset_job(
    name="normalized_dbt_assets_job", 
    selection="tag:timestamp_normalized",
    description="Runs the data normalization dbt models"
)

# Define a Dagster job that uses the dbt assets
# run tag:latest dbt assets
latest_dbt_assets_job = define_asset_job(
    name="latest_dbt_assets_job", 
    selection="tag:latest_clean_data",
    description="Runs the get latest data dbt models"
)

# Define a Dagster job that uses the dbt assets
# run tag:period_change_data dbt assets
period_change_data_dbt_assets_job = define_asset_job(
    name="period_change_data_dbt_assets_job", 
    selection="tag:period_change_data",
    description="Runs the period change data dbt models to create views"
)