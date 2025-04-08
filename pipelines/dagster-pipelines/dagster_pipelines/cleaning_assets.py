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
from dagster_dbt import DbtCliResource, DagsterDbtTranslator, dbt_assets
from dagster import asset, AssetExecutionContext, AssetKey
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
    staging_table_name = "latest_project_repos_contributors_staging"
    final_table_name = "latest_project_repos_contributors"
    old_table_name = "latest_project_repos_contributors_old"
    schema_name = "clean"

    def run_validations(context, df: pd.DataFrame, engine):
        """Runs validation checks on the DataFrame against the existing final table. Raises ValueError if checks fail."""
        context.log.info("Running validations...")

        context.log.info("Checking if DataFrame is empty...")
        if df.empty:
            raise ValueError("Validation failed: Input DataFrame is empty.")
        context.log.info("DataFrame is not empty.")

        context.log.info("Checking if 'repo' column contains NULL values...")
        if df['repo'].isnull().any():
            raise ValueError("Validation failed: 'repo' column contains NULL values.")
        context.log.info("'repo' column does not contain NULL values.")

        existing_record_count_val = None
        existing_data_timestamp_val = None

        try:
            context.log.info(f"Checking existing data in {schema_name}.{final_table_name}...")
            with engine.connect() as conn:
                # Check if final table exists first
                table_exists_result = conn.execute(text(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{final_table_name}');"
                ))
                final_table_exists = table_exists_result.scalar_one() # Use scalar_one for single boolean result

                if final_table_exists:
                    result_count = conn.execute(text(f"SELECT COUNT(*) FROM {schema_name}.{final_table_name}"))
                    existing_record_count_val = result_count.scalar()

                    result_ts = conn.execute(text(f"SELECT MAX(data_timestamp) FROM {schema_name}.{final_table_name}"))
                    existing_data_timestamp_val = result_ts.scalar() # Can be None/NaT if table empty/no timestamp
                else:
                    context.log.warning(f"Final table {schema_name}.{final_table_name} does not exist. Skipping comparison checks.")
                    # Allow first run where the table doesn't exist yet

        except Exception as e:
            context.log.error(f"Error accessing existing data in {schema_name}.{final_table_name}: {e}", exc_info=True)
            # If comparison is essential, raise. If optional on first run, just warn. Let's raise.
            raise ValueError(f"Validation failed: Error accessing existing data in {schema_name}.{final_table_name}.")

        # --- Comparison Validations (only if existing data was found) ---
        if existing_record_count_val is not None:
            context.log.info(f"Checking record count deviation (New: {df.shape[0]}, Existing: {existing_record_count_val})...")
            if existing_record_count_val > 0:
                deviation = abs(df.shape[0] - existing_record_count_val) / existing_record_count_val
                if deviation > 0.5:
                    raise ValueError(f"Validation failed: Record count deviation ({deviation:.1%}) exceeds 50%.")
                context.log.info(f"Record count deviation ({deviation:.1%}) within 50% threshold.")
            elif df.shape[0] > 0:
                context.log.warning("Existing table had 0 records, new data has records.")
            else: # Both 0
                context.log.info("Both existing and new data appear empty.")
                raise ValueError("Validation failed: Both existing and new data appear empty.")


        if existing_data_timestamp_val is not None and not pd.isna(existing_data_timestamp_val):
            # Timestamps need to be timezone-aware for proper comparison
            now_ts = pd.Timestamp.now(tz='UTC') # Assuming UTC, adjust if necessary
            existing_data_timestamp_val_aware = pd.Timestamp(existing_data_timestamp_val).tz_localize('UTC') # Assuming stored as naive UTC

            context.log.info(f"Checking data timestamp (Existing: {existing_data_timestamp_val_aware}, Threshold: 25 days)...")
            # Check if existing data timestamp is EARLIER than 25 days ago (i.e., older than 25 days)
            if existing_data_timestamp_val_aware > (now_ts - pd.Timedelta(days=25)):
                # Raise error only if data is NEWER than 25 days
                raise ValueError(f"Validation failed: Existing data timestamp ({existing_data_timestamp_val_aware}) is not older than 25 days.")
            context.log.info("Existing data timestamp is more than 25 days old.")
        elif final_table_exists: # Only warn if table existed but timestamp was null/missing
            context.log.warning("Could not retrieve a valid existing data timestamp for comparison.")

        context.log.info("Validations passed.")
        return True

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

            if rows.empty:
                raise ValueError("Validation failed: DataFrame is empty.")
                return dg.MaterializeResult(metadata={"row_count": 0, "message": "No raw data found."})

            # capture the data in a list
            print(f"Fetched {len(rows)} repos with compressed data. Decompressing...")
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

        if not data:
            print("Data list is empty after processing raw rows.")
            raise ValueError("Validation failed: Data is empty.")

        # write the data to a pandas dataframe
        contributors_df = pd.DataFrame(data)

        # add unix datetime column
        contributors_df['data_timestamp'] = pd.Timestamp.now()
        print(f"Created decompressed dataframe with {len(contributors_df)} rows.")

        # Run Validations (Comparing processed data against current FINAL table)
        run_validations(context, contributors_df, cloud_sql_engine)

        # Write DataFrame to Staging Table first
        context.log.info(f"Writing data to staging table {schema_name}.{staging_table_name}...")
        contributors_df.to_sql(
            staging_table_name,
            cloud_sql_engine,
            if_exists='replace', # Replace staging table safely
            index=False,
            schema=schema_name,
            chunksize=10000 # Good for large dataframes
        )
        print("Successfully wrote to staging table.")

        # Perform Atomic Swap via Transaction
        print(f"Validation passed. Performing atomic swap to update {schema_name}.{final_table_name}...")
        with cloud_sql_engine.connect() as conn:
            with conn.begin(): # Start transaction
                # Use CASCADE if Foreign Keys might point to the table
                print(f"Dropping old table {schema_name}.{old_table_name} if it exists...")
                conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{old_table_name} CASCADE;"))

                print(f"Renaming current {schema_name}.{final_table_name} to {schema_name}.{old_table_name} (if it exists)...")
                conn.execute(text(f"ALTER TABLE IF EXISTS {schema_name}.{final_table_name} RENAME TO {old_table_name};"))

                print(f"Renaming staging table {schema_name}.{staging_table_name} to {schema_name}.{final_table_name}...")
                conn.execute(text(f"ALTER TABLE {schema_name}.{staging_table_name} RENAME TO {final_table_name};"))
            # Transaction commits here if no exceptions were raised inside the 'with conn.begin()' block
        print("Atomic swap successful.")

        # cleanup old table (outside the main transaction)
        try:
            with cloud_sql_engine.connect() as conn:
                 conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{old_table_name} CASCADE;"))
                 print(f"Cleaned up table {schema_name}.{old_table_name}.")
        except Exception as cleanup_e:
            # Log warning - cleanup failure shouldn't fail the asset run
            print(f"Could not drop old table {schema_name}.{old_table_name}: {cleanup_e}")

        # Fetch Metadata from the FINAL table for MaterializeResult
        print("Fetching metadata for Dagster result...")
        with cloud_sql_engine.connect() as conn:
            row_count_result = conn.execute(text(f"SELECT COUNT(*) FROM {schema_name}.{final_table_name}"))
            # Use scalar_one() for single value, assumes table not empty after swap
            row_count = row_count_result.scalar_one()

            preview_result = conn.execute(text(f"SELECT * FROM {schema_name}.{final_table_name} LIMIT 10"))
            # Fetch into dicts using .mappings().all() for easy DataFrame creation
            result_df = pd.DataFrame(preview_result.mappings().all())

        print(f"Asset materialization complete. Final row count: {row_count}")
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)) if not result_df.empty else "No rows found for preview.",
                "message": "Data processed and table updated successfully."
            }
        )
    # --- Exception Handling for the entire asset function ---
    except ValueError as ve:
        print(f"Validation error: {ve}", exc_info=True)
        raise ve # Re-raise to fail the Dagster asset run clearly indicating validation failure

    except Exception as e:
        print(f"An unexpected error occurred: {e}", exc_info=True)
        raise e # Re-raise any other exception to fail the Dagster asset run
########################################################################################################################