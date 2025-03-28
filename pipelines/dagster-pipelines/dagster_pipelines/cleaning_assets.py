import os

from pathlib import Path
import time
import dagster as dg
import pandas as pd
from sqlalchemy import text
import requests
import json
import random
from dagster_pipelines.assets import github_project_repos_contributors
from dagster_dbt import DbtCliResource, dbt_assets
from dagster import asset, AssetExecutionContext
from dagster_pipelines.resources import dbt_resource
import gzip
import psycopg2

################################################ normalized time series data ################################################
@dbt_assets(
    manifest="/home/builder-love/data/pipelines/dbt-pipelines/dbt_pipelines/target/manifest.json",
    select="fqn:*"  # Select ALL dbt resources
)
def all_dbt_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["run"], context=context).stream()
    yield from dbt_resource.cli(["test"], context=context).stream()

# to select a specific dbt asset, use the following code
# @dbt_assets(
#     manifest="/home/builder-love/data/pipelines/dbt-pipelines/dbt_pipelines/target/manifest.json",
#     select="fqn:normalized_project_organizations"  # Select ALL dbt resources
# )
# def all_dbt_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
#     yield from dbt_resource.cli(["run", "--select", "fqn:normalized_project_organizations"], context=context).stream()
#     yield from dbt_resource.cli(["test", "--select", "fqn:normalized_project_organizations"], context=context).stream()

########################################################################################################################


################################################ process compressed data #######################################################

@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="clean_data",
    deps=[github_project_repos_contributors],
    automation_condition=dg.AutomationCondition.eager(),
)
def process_compressed_contributors_data(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # Execute the query
    # Extracts, decompresses, and inserts data into the clean table.
    try:
        with cloud_sql_engine.connect() as conn:
            result = conn.execute(text(
                """
                SELECT repo, contributor_list
                FROM raw.project_repos_contributors
                WHERE data_timestamp = (SELECT MAX(data_timestamp) FROM raw.project_repos_contributors);
                """
            ))
            rows = pd.DataFrame(result.fetchall(), columns=result.keys())

            # capture the data in a list
            data = []
            for repo, compressed_data in rows.itertuples(index=False):

                # hex to bytes and then decompress bytes
                # Remove the '\x' prefixes and convert to bytes
                hex_string_no_prefix = compressed_data.replace("\\x", "")
                byte_data = bytes.fromhex(hex_string_no_prefix)

                # Decompress the data - returns json byte string
                contributors_json_string = gzip.decompress(byte_data)

                # Parse the JSON string into a list of dictionaries
                contributors_list = json.loads(contributors_json_string)

                for contributor in contributors_list:
                    if contributor['type'] != 'Anonymous':
                        data.append({
                            "repo": repo,
                            "contributor_login": contributor['login'],
                            "contributor_id": contributor.get('id'),
                            "contributor_node_id": contributor.get('node_id'),
                            "contributor_avatar_url": contributor.get('avatar_url'),
                            "contributor_gravatar_id": contributor.get('gravatar_id'),
                            "contributor_url": contributor.get('url'),
                            "contributor_html_url": contributor.get('html_url'),
                            "contributor_followers_url": contributor.get('followers_url'),
                            "contributor_following_url": contributor.get('following_url'),
                            "contributor_gists_url": contributor.get('gists_url'),
                            "contributor_starred_url": contributor.get('starred_url'),
                            "contributor_subscriptions_url": contributor.get('subscriptions_url'),
                            "contributor_organizations_url": contributor.get('organizations_url'),
                            "contributor_repos_url": contributor.get('repos_url'),
                            "contributor_events_url": contributor.get('events_url'),
                            "contributor_received_events_url": contributor.get('received_events_url'),
                            "contributor_type": contributor.get('type'),
                            "contributor_user_view_type": contributor.get('user_view_type'),
                            "contributor_site_admin": contributor.get('site_admin'),
                            "contributor_contributions": contributor.get('contributions'),
                            "contributor_email": '',
                        })
                    else:
                        data.append({
                            "repo": repo,
                            "contributor_login": contributor['name'],
                            "contributor_id": '',
                            "contributor_node_id": '',
                            "contributor_avatar_url": '',
                            "contributor_gravatar_id": '',
                            "contributor_url": '',
                            "contributor_html_url": '',
                            "contributor_followers_url": '',
                            "contributor_following_url": '',
                            "contributor_gists_url": '',
                            "contributor_starred_url": '',
                            "contributor_subscriptions_url": '',
                            "contributor_organizations_url": '',
                            "contributor_repos_url": '',
                            "contributor_events_url": '',
                            "contributor_received_events_url": '',
                            "contributor_type": contributor.get('type'),
                            "contributor_user_view_type": '',
                            "contributor_site_admin": '',
                            "contributor_contributions": contributor['contributions'],
                            "contributor_email": contributor['email'],
                        })
    except psycopg2.Error as e:
        print(f"Error processing data: {e}")
        conn.rollback()

    # write the data to a pandas dataframe
    contributors_df = pd.DataFrame(data)

    # add unix datetime column
    contributors_df['data_timestamp'] = pd.Timestamp.now()

    # write the data to the clean.project_repos_contributors table
    contributors_df.to_sql('latest_project_repos_contributors', cloud_sql_engine, if_exists='replace', index=False, schema='clean')

    with cloud_sql_engine.connect() as conn:
        # # capture asset metadata
        preview_query = text("select count(*) from clean.latest_project_repos_contributors")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from clean.latest_project_repos_contributors")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )

########################################################################################################################