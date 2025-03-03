import dagster as dg
import pandas as pd
from sqlalchemy import text
from dagster_pipelines.assets import crypto_ecosystems_project_toml_files, github_project_orgs, github_project_sub_ecosystems, github_project_repos

# define the asset that gets the latest project toml files from the project_toml_files table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="latest_data",
    deps=[crypto_ecosystems_project_toml_files],
    automation_condition=dg.AutomationCondition.eager(),
)
def latest_project_toml_files(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    with cloud_sql_engine.connect() as conn:

        # create latest project toml files data table with the transformed data
        query = text("""
            DROP TABLE IF EXISTS latest_project_toml_files;
            CREATE TABLE IF NOT EXISTS latest_project_toml_files AS
            SELECT toml_file_data_url, data_timestamp
            FROM project_toml_files
            WHERE data_timestamp = (SELECT MAX(data_timestamp) FROM project_toml_files);
        """)

        # Execute the query
        conn.execute(query)
        conn.commit()

        # capture asset metadata
        preview_query = text("select count(*) from latest_project_toml_files")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from latest_project_toml_files limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )

# define the asset that gets the latest github orgs for a project from the project_organizations table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="latest_data",
    deps=[github_project_orgs],
    automation_condition=dg.AutomationCondition.eager(),
)
def latest_github_project_orgs(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    with cloud_sql_engine.connect() as conn:

        # create latest project organizations data table with the transformed data
        query = text("""
            DROP TABLE IF EXISTS latest_project_organizations;
            CREATE TABLE IF NOT EXISTS latest_project_organizations AS
            SELECT project_title, project_organization_url, data_timestamp
            FROM project_organizations
            WHERE data_timestamp = (SELECT MAX(data_timestamp) FROM project_organizations);
        """)

        # Execute the query
        conn.execute(query)
        conn.commit()

        # capture asset metadata
        preview_query = text("select count(*) from latest_project_organizations")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from latest_project_organizations limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )

# define the asset that gets the latest sub ecosystems for a project from the project_sub_ecosystems table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="latest_data",
    deps=[github_project_sub_ecosystems],
    automation_condition=dg.AutomationCondition.eager(),
)
def latest_github_project_sub_ecosystems(context) -> dg.MaterializeResult:

    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    with cloud_sql_engine.connect() as conn:

        # create latest project sub ecosystems data table with the transformed data
        query = text("""
            DROP TABLE IF EXISTS latest_project_sub_ecosystems;
            CREATE TABLE IF NOT EXISTS latest_project_sub_ecosystems AS
            SELECT project_title, sub_ecosystem, data_timestamp
            FROM project_sub_ecosystems
            WHERE data_timestamp = (SELECT MAX(data_timestamp) FROM project_sub_ecosystems);
        """)

        # Execute the query
        conn.execute(query)
        conn.commit()

        # capture asset metadata
        preview_query = text("select count(*) from latest_project_sub_ecosystems")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from latest_project_sub_ecosystems limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )

# define the asset that gets the latest repositories for a project from the project_repos table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="latest_data",
    deps=[github_project_repos],
    automation_condition=dg.AutomationCondition.eager(),
)
def latest_github_project_repos(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # create latest project repos data table with the transformed data
        query = text("""
            DROP TABLE IF EXISTS latest_project_repos;
            CREATE TABLE IF NOT EXISTS latest_project_repos AS
            SELECT project_title, repo, repo_source, data_timestamp,
            substring(repo from 'https://github.com/(.+)') AS repo_name
            FROM project_repos
            WHERE data_timestamp = (SELECT MAX(data_timestamp) FROM project_repos);
        """)

        conn.execute(query)
        conn.commit()

        # capture asset metadata
        preview_query = text("select count(*) from latest_project_repos")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from latest_project_repos limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )