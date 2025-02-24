import dagster as dg
import pandas as pd
from sqlalchemy import text
from dagster_pipelines.latest_assets import latest_github_project_repos

# define the asset that gets the distinct repo list from the latest_github_project_repos table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="clean_data",
    deps=[latest_github_project_repos],
    automation_condition=dg.AutomationCondition.eager(),
)
def distinct_github_project_repos(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # Execute the query
    with cloud_sql_engine.connect() as conn:
        
        # drop the table if it exists
        conn.execute(text("DROP TABLE IF EXISTS latest_distinct_project_repos;"))

        # create latest project toml files data table with the transformed data
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS latest_distinct_project_repos AS
            SELECT DISTINCT repo
            FROM latest_project_repos;
        """))

        # capture asset metadata
        preview_query = text("select count(*) from latest_distinct_project_repos")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from latest_distinct_project_repos limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )