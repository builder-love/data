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
from dagster_dbt import DbtCliResource, DagsterDbtTranslator, dbt_assets, DbtCliInvocation
from dagster import asset, AssetExecutionContext, AssetKey, op, Out, Output, job
from dagster_pipelines.resources import dbt_resource
import gzip
import psycopg2

################################################ normalized time series data ################################################

# this calculates the path relative to the current file (resources.py)
# assumes resources.py -> dagster_pipelines -> dagster-pipelines -> data -> dbt-pipelines/dbt_pipelines
_THIS_FILE_DIR = Path(__file__).parent.resolve()
_PROJECT_ROOT_PATH = _THIS_FILE_DIR.parent.parent
MANIFEST_PATH = _PROJECT_ROOT_PATH / "dbt-pipelines" / "dbt_pipelines" / "target" / "manifest.json"

# Define Custom Translator for dbt sources
class CustomDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        """
        Override this method to customize asset key generation.
        """
        # Get the default asset key first
        asset_key = super().get_asset_key(dbt_resource_props)

        # Check if the resource is a dbt source
        if dbt_resource_props["resource_type"] == "source":
            # Prepend 'sources' (or anything unique) to the key path
            # This changes ['clean', 'latest_project_repos'] to ['sources', 'clean', 'latest_project_repos']
            # for the source only.
            return AssetKey(["sources"] + asset_key.path) 

        # For all other resource types (like models), use the default key
        return asset_key

# --- Use the Translator in your dbt_assets definition ---
# Make sure DBT_MANIFEST_PATH points to your actual manifest.json
# If it doesn't exist, run `dbt build` or `dbt compile` in your dbt project first.
if MANIFEST_PATH.exists():
    @dbt_assets(
    manifest=MANIFEST_PATH,
    dagster_dbt_translator=CustomDbtTranslator(),
    select="fqn:*"  # Select ALL dbt resources
    )
    def all_dbt_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
        yield from dbt_resource.cli(["run"], context=context).stream()
        yield from dbt_resource.cli(["test"], context=context).stream()
else:
    # Handle case where manifest doesn't exist (e.g., define empty assets list or raise error)
    print(f"WARNING: dbt manifest not found at {MANIFEST_PATH}. Skipping dbt asset definition.")
    # Define an empty list or handle appropriately if dbt assets are optional
    all_dbt_assets = []

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

# --- Run Validations ---

# check dates before proceeding
def process_compressed_contributors_validation_dates(context, target_ts_aware_contributors, max_source_ts_aware, final_table_name_contributors, time_threshold: int = 20):

    context.log.info(f"Checking if the target data_timestamp ({target_ts_aware_contributors}) is less than {time_threshold} days since the source max data_timestamp ({max_source_ts_aware})...")

    # check if the target_ts_aware_contributors is less than 25 days since the max_source_ts_aware
    time_difference = max_source_ts_aware - target_ts_aware_contributors
    if time_difference < pd.Timedelta(days=time_threshold):
        context.log.warning(f"Validation failed for '{final_table_name_contributors}': Existing data timestamp ({target_ts_aware_contributors}) is less than {time_threshold} days since the max source data_timestamp ({max_source_ts_aware}). Difference: {time_difference}.")
        return False # Validation fails
    
    context.log.info(f"Timestamp validation passed for '{final_table_name_contributors}'. Difference: {time_difference}.")
    return True

# validation function for the compressed data
def run_validations(context, 
                    df_contributors: pd.DataFrame, 
                    df_project_repos_contributors: pd.DataFrame, 
                    engine, 
                    schema_name: str = "clean", 
                    final_table_name_contributors: str = "latest_contributors", 
                    final_table_name_project_repos_contributors: str = "latest_project_repos_contributors") -> bool:
    """Runs validation checks on the DataFrame against the existing final table."""
    context.log.info("Running validations...")

    context.log.info("Checking if DataFrames are empty...")
    if df_contributors.empty or df_project_repos_contributors.empty:
        raise ValueError("Validation failed: Input DataFrames are empty.")
    context.log.info("DataFrames are not empty.")

    context.log.info("Checking if 'contributor_unique_id_builder_love' or 'repo' column contains NULL values in either DataFrame...")
    if df_project_repos_contributors['repo'].isnull().any():
        raise ValueError("Validation failed: 'repo' column contains NULL values.")
    if df_contributors['contributor_unique_id_builder_love'].isnull().any() or df_project_repos_contributors['contributor_unique_id_builder_love'].isnull().any():
        raise ValueError("Validation failed: 'contributor_unique_id_builder_love' column contains NULL values.")
    context.log.info("'repo' and 'contributor_unique_id_builder_love' columns do not contain NULL values.")

    existing_record_count_val_contributors = None
    existing_record_count_val_project_repos_contributors = None

    try:
        context.log.info(f"Checking existing data in {schema_name}.{final_table_name_contributors} and {schema_name}.{final_table_name_project_repos_contributors}...")
        with engine.connect() as conn:
            table_exists_result_contributors = conn.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{final_table_name_contributors}');"
            ))
            final_table_exists_contributors = table_exists_result_contributors.scalar_one()

            table_exists_result_project_repos_contributors = conn.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{final_table_name_project_repos_contributors}');"
            ))
            final_table_exists_project_repos_contributors = table_exists_result_project_repos_contributors.scalar_one()

            if final_table_exists_contributors:
                result_count = conn.execute(text(f"SELECT COUNT(*) FROM {schema_name}.{final_table_name_contributors}"))
                existing_record_count_val_contributors = result_count.scalar()
            else:
                context.log.warning(f"Final table {schema_name}.{final_table_name_contributors} does not exist. Skipping comparison checks for this table.")

            if final_table_exists_project_repos_contributors:
                result_count = conn.execute(text(f"SELECT COUNT(*) FROM {schema_name}.{final_table_name_project_repos_contributors}"))
                existing_record_count_val_project_repos_contributors = result_count.scalar()
            else:
                context.log.warning(f"Final table {schema_name}.{final_table_name_project_repos_contributors} does not exist. Skipping comparison checks for this table.")
    except Exception as e:
        context.log.error(f"Error accessing existing data: {e}", exc_info=True)
        raise ValueError(f"Validation failed: Error accessing existing data.")

    # --- Record Count Comparison Validations ---
    if existing_record_count_val_contributors is not None: # Only if target table existed
        context.log.info(f"Checking record count deviation for '{final_table_name_contributors}' (New: {df_contributors.shape[0]}, Existing: {existing_record_count_val_contributors})...")
        if existing_record_count_val_contributors > 0:
            deviation = abs(df_contributors.shape[0] - existing_record_count_val_contributors) / existing_record_count_val_contributors
            if deviation > 0.5:
                raise ValueError(f"Validation failed for '{final_table_name_contributors}': Record count deviation ({deviation:.1%}) exceeds 50%.")
            context.log.info(f"Record count deviation for '{final_table_name_contributors}' ({deviation:.1%}) within 50% threshold.")
        elif df_contributors.shape[0] > 0: # Existing was 0, new has data
            context.log.info(f"Target table '{final_table_name_contributors}' had 0 records, new data has records. This is acceptable.")
        else: # Both 0
            context.log.info(f"Both existing and new data for '{final_table_name_contributors}' appear empty (0 records). Raising error.")
            raise ValueError(f"Validation failed for '{final_table_name_contributors}': Both existing and new data appear empty.")


    if existing_record_count_val_project_repos_contributors is not None: # Only if target table existed
        context.log.info(f"Checking record count deviation for '{final_table_name_project_repos_contributors}' (New: {df_project_repos_contributors.shape[0]}, Existing: {existing_record_count_val_project_repos_contributors})...")
        if existing_record_count_val_project_repos_contributors > 0:
            deviation = abs(df_project_repos_contributors.shape[0] - existing_record_count_val_project_repos_contributors) / existing_record_count_val_project_repos_contributors
            if deviation > 0.5:
                raise ValueError(f"Validation failed for '{final_table_name_project_repos_contributors}': Record count deviation ({deviation:.1%}) exceeds 50%.")
            context.log.info(f"Record count deviation for '{final_table_name_project_repos_contributors}' ({deviation:.1%}) within 50% threshold.")
        elif df_project_repos_contributors.shape[0] > 0: # Existing was 0, new has data
             context.log.info(f"Target table '{final_table_name_project_repos_contributors}' had 0 records, new data has records. This is acceptable.")
        else: # Both 0
            context.log.info(f"Both existing and new data for '{final_table_name_project_repos_contributors}' appear empty (0 records). Raising error.")
            raise ValueError(f"Validation failed for '{final_table_name_project_repos_contributors}': Both existing and new data appear empty.")

    context.log.info("All validations passed.")
    return True

@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="clean_data",
    deps=[github_project_repos_contributors],
    automation_condition=dg.AutomationCondition.eager(),
)
def process_compressed_contributors_data(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # Define table names constants for clarity and easy changes
    staging_table_name_contributors = "latest_contributors_staging"
    final_table_name_contributors = "latest_contributors"
    old_table_name_contributors = "latest_contributors_old"
    staging_table_name_project_repos_contributors = "latest_project_repos_contributors_staging"
    final_table_name_project_repos_contributors = "latest_project_repos_contributors"
    old_table_name_project_repos_contributors = "latest_project_repos_contributors_old"
    schema_name = "clean"

    # Execute the query
    # Extracts, decompresses, and inserts data into the clean table.
    try:
        with cloud_sql_engine.connect() as conn:

            # first get data_timestamp from source and target tables to compare
            result = conn.execute(text("select max(data_timestamp) from raw.project_repos_contributors"))
            max_source_ts_aware = result.scalar()
            result = conn.execute(text("select max(data_timestamp) from clean.latest_contributors"))
            target_ts_aware_contributors = result.scalar()

            # confirm max_source_ts_aware and target_ts_aware_contributors are not None
            if max_source_ts_aware is None or target_ts_aware_contributors is None:
                raise ValueError("Validation failed: max_source_ts_aware or target_ts_aware_contributors is None.")
                return dg.MaterializeResult(
                    metadata={
                        "latest_contributors_preview": "No rows found for preview.",
                        "latest_project_repos_contributors_preview": "No rows found for preview.",
                        "message": "Data is not valid."
                    }
                )

            datetime_validation = process_compressed_contributors_validation_dates(context, target_ts_aware_contributors, max_source_ts_aware, final_table_name_contributors)

            if not datetime_validation:
                context.log.warning("Validation failed: Data is not valid.")
                return dg.MaterializeResult(
                    metadata={
                        "latest_contributors_preview": "No rows found for preview.",
                        "latest_project_repos_contributors_preview": "No rows found for preview.",
                        "message": "Data is not valid."
                    }
                )
            context.log.info("Date validation passed. Proceeding with data extraction...")

            # get the data from the source table
            result = conn.execute(text(
                """
                SELECT repo, contributor_list -- select the bytea column data
                FROM raw.project_repos_contributors
                WHERE data_timestamp = (SELECT MAX(data_timestamp) FROM raw.project_repos_contributors)
                """
            ))
            rows = pd.DataFrame(result.fetchall(), columns=result.keys())

            if rows.empty:
                raise ValueError("Validation failed: DataFrame is empty.")
                return dg.MaterializeResult(metadata={"row_count": 0, "message": "No raw data found."})

            # capture the data in a list
            print(f"Fetched {len(rows)} repos with compressed data. Decompressing...")
            data = []
            for repo, compressed_byte_data in rows.itertuples(index=False):

                if compressed_byte_data is None:
                    print(f"Warning: Skipping repo {repo} due to NULL compressed data.")
                    continue # Skip this row if data is NUL

                # Decompress the data - returns json byte string
                try:
                    contributors_json_string = gzip.decompress(compressed_byte_data)
                except gzip.BadGzipFile as e:
                    print(f"Error decompressing data for repo {repo}: {e}. Skipping.")
                    continue # Skip this repo if decompression fails
                except Exception as e:
                    print(f"Unexpected error during decompression for repo {repo}: {e}. Skipping.")
                    continue # Skip on other unexpected errors

                # Parse the JSON string into a list of dictionaries
                try:
                    contributors_list = json.loads(contributors_json_string)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON for repo {repo}: {e}. Skipping.")
                    continue # Skip if JSON is invalid

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
                            "contributor_name": contributor.get('name'), # contributor.get('name') is not always present
                            "contributor_email": contributor.get('email'), # contributor.get('email') is not always present,
                            "contributor_unique_id_builder_love": f"{contributor.get('login')}|{contributor.get('id')}", # derived unique identifier for the contributor
                        })
                    else:
                        data.append({
                            "repo": repo,
                            "contributor_login": f"{contributor['name']}",
                            "contributor_id": None,
                            "contributor_node_id": None,
                            "contributor_avatar_url": None,
                            "contributor_gravatar_id": None,
                            "contributor_url": None,
                            "contributor_html_url": None,
                            "contributor_followers_url": None,
                            "contributor_following_url": None,
                            "contributor_gists_url": None,
                            "contributor_starred_url": None,
                            "contributor_subscriptions_url": None,
                            "contributor_organizations_url": None,
                            "contributor_repos_url": None,
                            "contributor_events_url": None,
                            "contributor_received_events_url": None,
                            "contributor_type": contributor.get('type'),
                            "contributor_user_view_type": None,
                            "contributor_site_admin": None,
                            "contributor_contributions": contributor['contributions'],
                            "contributor_name": contributor['name'],
                            "contributor_email": contributor['email'],
                            "contributor_unique_id_builder_love": f"{contributor.get('name')}|{contributor.get('email')}", # derived unique identifier for the contributor
                        })

        if not data:
            context.log.info("Data list is empty after processing raw rows. Raising error.")
            raise ValueError("Validation failed: Data is empty.")

        # write the data to a pandas dataframe
        contributors_df = pd.DataFrame(data)

        # get the current unix timestamp as an object so we can pass it to the dataframes
        data_timestamp = pd.Timestamp.now()

        # create two new dataframes from the contributors_df
        # one containing the columns: contributor_unique_id_builder_love, repo, contributor_contributions, data_timestamp
        latest_project_repos_contributors_columns = ['contributor_unique_id_builder_love', 'repo', 'contributor_contributions']
        # one containing the columns: contributor_unique_id_builder_love, contributor_login, contributor_id, contributor_node_id, contributor_avatar_url, contributor_gravatar_id, contributor_url, contributor_html_url, contributor_followers_url, contributor_following_url, contributor_gists_url, contributor_starred_url, contributor_subscriptions_url, contributor_organizations_url, contributor_site_admin, contributor_repos_url, contributor_events_url, contributor_received_events_url, contributor_type, contributor_user_view_type, contributor_name, contributor_email, data_timestamp
        latest_contributors_columns = ['contributor_unique_id_builder_love','contributor_login', 'contributor_id', 'contributor_node_id', 'contributor_avatar_url', 'contributor_gravatar_id', 'contributor_url', 'contributor_html_url', 'contributor_followers_url', 'contributor_following_url', 'contributor_gists_url', 'contributor_starred_url', 'contributor_subscriptions_url', 'contributor_organizations_url', 'contributor_site_admin', 'contributor_repos_url', 'contributor_events_url', 'contributor_received_events_url', 'contributor_type', 'contributor_user_view_type', 'contributor_name', 'contributor_email']

        # create the new dataframes
        latest_project_repos_contributors_df = contributors_df[latest_project_repos_contributors_columns]
        latest_contributors_df = contributors_df[latest_contributors_columns]

        # print the number of rows in the dataframes
        if len(contributors_df) > 0 and len(latest_project_repos_contributors_df) > 0 and len(latest_contributors_df) > 0:
            print(f"Created decompressed dataframe with {len(contributors_df)} rows.")
            print(f"Created latest_project_repos_contributors_df with {len(latest_project_repos_contributors_df)} rows.")
            print(f"Created latest_contributors_df with {len(latest_contributors_df)} rows.")
            if len(latest_project_repos_contributors_df) != len(latest_contributors_df) != len(contributors_df):
                raise ValueError("Validation failed: Dataframes are not the same size.")
            else:
                print("All dataframes have the same number of rows.")
                print("Dropping old dataframes to free up memory...")
                del contributors_df
        else:
            raise ValueError("Validation failed: Dataframes are empty.")

        # first update the unix timestamp so that both dataframes have the same data_timestamp
        # add the data_timestamp to the dataframes
        latest_project_repos_contributors_df['data_timestamp'] = data_timestamp
        latest_contributors_df['data_timestamp'] = data_timestamp

        # drop full duplicates from the latest_contributors_df
        # this table represents the unique contributors across all repos
        latest_contributors_df = latest_contributors_df.drop_duplicates()

        # Run Validations (Comparing processed data against current FINAL table)
        validations_passed = run_validations(context, latest_contributors_df, latest_project_repos_contributors_df, cloud_sql_engine, schema_name, final_table_name_contributors, final_table_name_project_repos_contributors)
        
        if not validations_passed:
            context.log.warning("Validation failed: Data is not valid.")
            return dg.MaterializeResult(
                metadata={
                    "latest_contributors_preview": "No rows found for preview.",
                    "latest_project_repos_contributors_preview": "No rows found for preview.",
                    "message": "Data is not valid."
                }
            )
        print("All validations passed. Proceeding with data processing...")

        # Write DataFrame to Staging Tables first
        # write to two tables here: latest_project_repos_contributors and latest_contributors
        context.log.info(f"Writing data to staging tables {schema_name}.{staging_table_name_contributors} and {schema_name}.{staging_table_name_project_repos_contributors}...")
        latest_contributors_df.to_sql(
            staging_table_name_contributors,
            cloud_sql_engine,
            if_exists='replace', # Replace staging table safely
            index=False,
            schema=schema_name,
            chunksize=50000 # Good for large dataframes
        )
        latest_project_repos_contributors_df.to_sql(
            staging_table_name_project_repos_contributors,
            cloud_sql_engine,
            if_exists='replace', # Replace staging table safely
            index=False,
            schema=schema_name,
            chunksize=50000 # Good for large dataframes
        )
        print("Successfully wrote to staging tables.")

        # Perform Atomic Swap via Transaction
        context.log.info(f"Performing atomic swap to update {schema_name}.{final_table_name_contributors} and {schema_name}.{final_table_name_project_repos_contributors}...")
        with cloud_sql_engine.connect() as conn:
            with conn.begin(): # Start transaction
                # Use CASCADE if Foreign Keys might point to the table
                print(f"Dropping old tables {schema_name}.{old_table_name_contributors} and {schema_name}.{old_table_name_project_repos_contributors} if they exist...")
                conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{old_table_name_contributors} CASCADE;"))
                conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{old_table_name_project_repos_contributors} CASCADE;"))

                print(f"Renaming current {schema_name}.{final_table_name_contributors} to {schema_name}.{old_table_name_contributors} (if it exists)...")
                conn.execute(text(f"ALTER TABLE IF EXISTS {schema_name}.{final_table_name_contributors} RENAME TO {old_table_name_contributors};"))
                print(f"Renaming current {schema_name}.{final_table_name_project_repos_contributors} to {schema_name}.{old_table_name_project_repos_contributors} (if it exists)...")
                conn.execute(text(f"ALTER TABLE IF EXISTS {schema_name}.{final_table_name_project_repos_contributors} RENAME TO {old_table_name_project_repos_contributors};"))

                print(f"Renaming staging tables {schema_name}.{staging_table_name_contributors} and {schema_name}.{staging_table_name_project_repos_contributors} to {schema_name}.{final_table_name_contributors} and {schema_name}.{final_table_name_project_repos_contributors}...")
                conn.execute(text(f"ALTER TABLE {schema_name}.{staging_table_name_contributors} RENAME TO {final_table_name_contributors};"))
                conn.execute(text(f"ALTER TABLE {schema_name}.{staging_table_name_project_repos_contributors} RENAME TO {final_table_name_project_repos_contributors};"))
            # Transaction commits here if no exceptions were raised inside the 'with conn.begin()' block
        context.log.info("Atomic swap successful.")

        # cleanup old table (outside the main transaction)
        context.log.info(f"Cleaning up old tables {schema_name}.{old_table_name_contributors} and {schema_name}.{old_table_name_project_repos_contributors}...")
        try:
            with cloud_sql_engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{old_table_name_contributors} CASCADE;"))
                    conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{old_table_name_project_repos_contributors} CASCADE;"))
                    context.log.info(f"Cleaned up tables {schema_name}.{old_table_name_contributors} and {schema_name}.{old_table_name_project_repos_contributors}.")
        except Exception as cleanup_e:
            # Log warning - cleanup failure shouldn't fail the asset run
            context.log.warning(f"Could not drop old tables {schema_name}.{old_table_name_contributors} and {schema_name}.{old_table_name_project_repos_contributors}: {cleanup_e}")

        # recreate the indexes for both tables
        try:
            with cloud_sql_engine.connect() as conn:
                with conn.begin():
                    # create the unique_id index
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_latest_contributors_unique_id ON {schema_name}.{final_table_name_contributors} (contributor_unique_id_builder_love);"))
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_latest_project_repos_contributors_unique_id ON {schema_name}.{final_table_name_project_repos_contributors} (contributor_unique_id_builder_love);"))

                    # create repo index
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_latest_project_repos_contributors_repo ON {schema_name}.{final_table_name_project_repos_contributors} (repo);"))

                    # analyze the tables to ensure the indexes are used
                    conn.execute(text(f"ANALYZE {schema_name}.{final_table_name_contributors};"))
                    conn.execute(text(f"ANALYZE {schema_name}.{final_table_name_project_repos_contributors};"))
        except Exception as index_e:
            print(f"Could not create indexes for {schema_name}.{final_table_name_contributors} and {schema_name}.{final_table_name_project_repos_contributors}: {index_e}")
            raise e

        # Fetch Metadata from the FINAL table for MaterializeResult
        print("Fetching metadata for Dagster result...")
        with cloud_sql_engine.connect() as conn:
            row_count_result = conn.execute(text(f"SELECT COUNT(*) FROM {schema_name}.{final_table_name_contributors}"))
            row_count_result_project_repos_contributors = conn.execute(text(f"SELECT COUNT(*) FROM {schema_name}.{final_table_name_project_repos_contributors}"))
            # Use scalar_one() for single value, assumes table not empty after swap
            row_count = row_count_result.scalar_one()
            row_count_project_repos_contributors = row_count_result_project_repos_contributors.scalar_one()

            preview_result = conn.execute(text(f"SELECT * FROM {schema_name}.{final_table_name_contributors} LIMIT 10"))
            preview_result_project_repos_contributors = conn.execute(text(f"SELECT * FROM {schema_name}.{final_table_name_project_repos_contributors} LIMIT 10"))
            # Fetch into dicts using .mappings().all() for easy DataFrame creation
            result_df = pd.DataFrame(preview_result.mappings().all())
            result_df_project_repos_contributors = pd.DataFrame(preview_result_project_repos_contributors.mappings().all())

        print(f"Asset materialization complete. Final row count: {row_count}")
        print(f"Asset materialization complete. Final row count: {row_count_project_repos_contributors}")
        return dg.MaterializeResult(
            metadata={
                "latest_contributors_row_count": dg.MetadataValue.int(row_count),
                "latest_project_repos_contributors_row_count": dg.MetadataValue.int(row_count_project_repos_contributors),
                "latest_contributors_preview": dg.MetadataValue.md(result_df.to_markdown(index=False)) if not result_df.empty else "No rows found for preview.",
                "latest_project_repos_contributors_preview": dg.MetadataValue.md(result_df_project_repos_contributors.to_markdown(index=False)) if not result_df_project_repos_contributors.empty else "No rows found for preview.",
                "message": "Data processed and table updated successfully."
            }
        )

    # --- Exception Handling for the entire asset function ---
    except ValueError as ve:
        context.log.error(f"Validation error: {ve}", exc_info=True)
        raise ve # Re-raise to fail the Dagster asset run clearly indicating validation failure

    except Exception as e:
        context.log.error(f"An unexpected error occurred: {e}", exc_info=True)
        raise e # Re-raise any other exception to fail the Dagster asset run
########################################################################################################################


########################################################################################################################
# crypto ecosystems raw file assets
########################################################################################################################

@op(required_resource_keys={"dbt_resource"}, out={"dbt_test_results": Out(bool)})
def run_dbt_tests_on_crypto_ecosystems_raw_file_staging(context) -> bool: #Correct return type
    """Runs dbt tests against the crypto_ecosystems_raw_file_staging schema."""
    context.log.info("Running dbt tests on crypto_ecosystems_raw_file_staging schema.")
    try:
        invocation: DbtCliInvocation = context.resources.dbt_resource.cli(
            ["test", "--select", "path:tests/crypto_ecosystems/"], context=context
        ).wait()
        print(f"dbt test invocation stdout: {type(invocation)}")
        print(f"dbt test invocation stdout: {invocation}")
        print(f"the returncode is {invocation.process.returncode}")
        if invocation.process.returncode == 0:  # Correct way to check for success
            context.log.info("dbt tests on crypto_ecosystems_raw_file_staging schema passed.")
            yield Output(True, output_name="dbt_test_results")
        else:
            context.log.info(f"dbt test invocation stdout: {invocation._stdout}")
            context.log.error(f"dbt test invocation stdout: {invocation._error_messages}") #Correct way to get output
            yield Output(False, output_name="dbt_test_results") # Still return a value

    except Exception as e:
        context.log.error(f"Error running dbt tests: {e}")
        yield Output(False, output_name="dbt_test_results")  # Consistent return type

# perform the DML work
# load new data from crypto_ecosystems_raw_file_staging table into crypto_ecosystems_raw_file table, and archive any data that is not in staging table
@op(
    required_resource_keys={"cloud_sql_postgres_resource"}
)
def load_new_data_from_staging_to_final(context, _):
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        dml_query = text("""
            -- Step 1a: Identify the primary keys (or ctids) of rows to delete ONCE
            CREATE TEMP TABLE IF NOT EXISTS tmp_keys_to_delete AS
            SELECT main.id 
            FROM raw.crypto_ecosystems_raw_file main
            LEFT JOIN raw.crypto_ecosystems_raw_file_staging sk
                ON main.project_title = sk.project_title
                AND main.sub_ecosystems IS NOT DISTINCT FROM sk.sub_ecosystems
                AND main.repo = sk.repo
                AND main.tags IS NOT DISTINCT FROM sk.tags
            WHERE sk.repo IS NULL;

            -- Step 1b: Archive rows by joining main table to the keys identified
            INSERT INTO raw.crypto_ecosystems_raw_file_archive (
                id, project_title, sub_ecosystems, repo, tags, data_timestamp,
                archived_at
            )
            SELECT
                main.id, main.project_title, main.sub_ecosystems, main.repo, main.tags, main.data_timestamp,
                NOW() AT TIME ZONE 'utc'
            FROM raw.crypto_ecosystems_raw_file main
            JOIN tmp_keys_to_delete keys_td ON main.id = keys_td.id; -- Join using the key

            -- Step 1c: Delete rows from main using the identified keys (MUCH more efficient)
            DELETE FROM raw.crypto_ecosystems_raw_file main
            WHERE main.id IN (SELECT id FROM tmp_keys_to_delete); -- Use simple IN clause with keys

            -- Step 1d: Clean up the temporary table
            DROP TABLE tmp_keys_to_delete;

            -- 2. Insert new rows (those in staging but not having an exact match in main)
            INSERT INTO raw.crypto_ecosystems_raw_file (
                project_title, sub_ecosystems, repo, tags, data_timestamp -- list all columns
            )
            SELECT
                stg.project_title, stg.sub_ecosystems, stg.repo, stg.tags, stg.data_timestamp -- select all columns
            FROM raw.crypto_ecosystems_raw_file_staging stg
            LEFT JOIN raw.crypto_ecosystems_raw_file main -- Note: Join target table changed from example
                ON stg.project_title = main.project_title
                AND stg.sub_ecosystems IS NOT DISTINCT FROM main.sub_ecosystems
                AND stg.repo = main.repo
                AND stg.tags IS NOT DISTINCT FROM main.tags

            WHERE main.repo IS NULL; -- If any part of the composite key in main is NULL, it means no match was found
        """)
        conn.execute(dml_query)
        conn.commit()
    context.log.info("Crypto ecosystems raw file data updates loaded from staging to final. Any data that is not in staging table is archived from the final table.")
    yield Output(None) # Use yield

@job()
def update_crypto_ecosystems_raw_file_job():
    """
    Tests the staging data and then updates the crypto_ecosystems_raw_file table.
    """
    # Execute tests first. If this op raises an Exception (fails), the job stops.
    test_results_passed = run_dbt_tests_on_crypto_ecosystems_raw_file_staging()

    # Define that load_new_data depends on the test results.
    # Pass the boolean result just for completeness or potential internal checks,
    # but the main control flow comes from the dependency + potential failure in the test op.
    load_new_data_from_staging_to_final(test_results_passed)
    # Alternatively, use start_after for pure control flow:
    # load_new_data_from_staging_to_final(start_after=test_results_passed)
