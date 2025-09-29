import os
from typing import Any, Dict, Mapping, Optional
import dagster as dg
import pandas as pd
from sqlalchemy import text
import json
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets, DbtCliResource
from dagster import AssetKey, op, Out, Output, In, Nothing, AssetMaterialization, MetadataValue, AssetExecutionContext, Config
from .resources import dbt_stg_resource, dbt_prod_resource
import gzip
from pathlib import Path
from sqlalchemy import inspect

# Define a simple, empty Config class for assets that don't need specific config keys.
class EmptyConfig(Config):
    pass

# get the environment and set configs
# Read the environment variable, defaulting to 'stg' for local development
env = os.getenv("DAGSTER_ENV", "stg").lower()

# Define the path to the dbt project within the container
# This should be the directory that contains the dbt_project.yml
dbt_project_dir = Path("/opt/dagster/app/dbt-pipelines")
manifest_path = None
translator = None

print(f"INFO: Running in '{env}' environment. Configuring dbt assets...")

# --- Custom DagsterDbtTranslator ---
# Define Custom Translator for dbt sources (can be reused)
class CustomDbtTranslator(DagsterDbtTranslator):
    
    def __init__(self, environment_prefix: str):
        self.environment_prefix = environment_prefix  # e.g., "prod" or "stg"
        super().__init__()

    def get_asset_key(self, dbt_node_info: Dict[str, Any]) -> AssetKey:
        """
        Prefixes the default dbt asset key with the environment name.
        e.g., AssetKey(["my_model"]) -> AssetKey(["stg", "my_model"])
        """
        base_key = super().get_asset_key(dbt_node_info)
        return base_key.with_prefix(self.environment_prefix)

    def get_tags(self, dbt_node_info: Dict[str, Any]) -> Optional[Mapping[str, str]]:
        dagster_tags = super().get_tags(dbt_node_info) or {}
        dbt_model_tags = dbt_node_info.get("tags", [])
        for tag in dbt_model_tags:
            dagster_tags[tag] = ""
        return dagster_tags if dagster_tags else None

    def get_group_name(self, dbt_node_info: Dict[str, Any]) -> Optional[str]:
        """
        Assigns all dbt assets for an environment to a single group.
        e.g., "dbt_stg" or "dbt_prod"
        """
        return f"dbt_{self.environment_prefix}"

# --- Instantiate dbt Translators ---
if env == "prod":
    # This path matches the 'target-path' in your dbt_project.yml for the 'prod' target
    manifest_path = dbt_project_dir / "target/prod" / "manifest.json"
    translator = CustomDbtTranslator(environment_prefix="prod")
elif env == "stg":
    # This path matches the 'target-path' for the 'stg' target
    manifest_path = dbt_project_dir / "target/stg" / "manifest.json"
    translator = CustomDbtTranslator(environment_prefix="stg")

# --- Conditionally define dbt Assets ---

# This is the single variable that Dagster Definitions object will import. Must update this across other files.
all_dbt_assets = []

# Check if the configuration was valid AND the manifest file actually exists
if manifest_path and translator and manifest_path.exists():

    @dbt_assets(
        manifest=manifest_path,
        dagster_dbt_translator=translator
    )
    def dbt_assets_for_env(context: AssetExecutionContext, dbt_cli: DbtCliResource):
        """
        Dynamically loads dbt assets based on the DAGSTER_ENV.
        """
        yield from dbt_cli.cli(["run"], context=context).stream()
        yield from dbt_cli.cli(["test"], context=context).stream()

    # Assign the newly defined assets to the variable.
    all_dbt_assets = dbt_assets_for_env

else:
    # This block runs if env is not 'prod'/'stg', or if the manifest is missing.
    print(f"WARNING: dbt manifest not found for environment '{env}' at expected path '{manifest_path}'.")
    print("Skipping dbt asset definition. Ensure you have run 'dbt compile' for the correct target in your Docker build.")

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
                    new_contributors_count: int, 
                    new_project_repos_contributors_count: int, 
                    engine, 
                    # This parameter now receives the schema where the target table lives (raw)
                    target_schema: str,
                    final_table_name_contributors: str = "latest_contributors", 
                    final_table_name_project_repos_contributors: str = "latest_project_repos_contributors") -> bool:
    """Runs validation checks using record counts against the existing final table."""
    
    context.log.info("-------------************** Running validations on record counts **************-------------")

    existing_record_count_val_contributors = 0
    existing_record_count_val_project_repos_contributors = 0

    try:
        context.log.info(f"Checking existing data in {target_schema}.{final_table_name_contributors} and {target_schema}.{final_table_name_project_repos_contributors}...")
        with engine.connect() as conn:
            # Check for contributors table
            if conn.dialect.has_table(conn, final_table_name_contributors, schema=target_schema):
                result_count = conn.execute(text(f"SELECT COUNT(*) FROM {target_schema}.{final_table_name_contributors}"))
                existing_record_count_val_contributors = result_count.scalar()
            else:
                context.log.warning(f"Final table {target_schema}.{final_table_name_contributors} does not exist. Assuming 0 records.")

            # Check for project_repos_contributors table
            if conn.dialect.has_table(conn, final_table_name_project_repos_contributors, schema=target_schema):
                result_count = conn.execute(text(f"SELECT COUNT(*) FROM {target_schema}.{final_table_name_project_repos_contributors}"))
                existing_record_count_val_project_repos_contributors = result_count.scalar()
            else:
                context.log.warning(f"Final table {target_schema}.{final_table_name_project_repos_contributors} does not exist. Assuming 0 records.")
    except Exception as e:
        context.log.error(f"Error accessing existing data: {e}", exc_info=True)
        raise ValueError(f"Validation failed: Error accessing existing data.")

    # --- Record Count Comparison Validations ---
    context.log.info(f"Checking record count deviation for '{final_table_name_contributors}' (New: {new_contributors_count}, Existing: {existing_record_count_val_contributors})...")
    if existing_record_count_val_contributors > 0:
        deviation = abs(new_contributors_count - existing_record_count_val_contributors) / existing_record_count_val_contributors
        if deviation > 0.5:
            raise ValueError(f"Validation failed for '{final_table_name_contributors}': Record count deviation ({deviation:.1%}) exceeds 50%.")
        context.log.info(f"Record count deviation for '{final_table_name_contributors}' ({deviation:.1%}) within 50% threshold.")
    elif new_contributors_count > 0:
        context.log.info(f"Target table '{final_table_name_contributors}' had 0 records, new data has records. This is acceptable.")
    else:
        raise ValueError(f"Validation failed for '{final_table_name_contributors}': Both existing and new data appear empty (0 records).")

    context.log.info(f"Checking record count deviation for '{final_table_name_project_repos_contributors}' (New: {new_project_repos_contributors_count}, Existing: {existing_record_count_val_project_repos_contributors})...")
    if existing_record_count_val_project_repos_contributors > 0:
        deviation = abs(new_project_repos_contributors_count - existing_record_count_val_project_repos_contributors) / existing_record_count_val_project_repos_contributors
        if deviation > 0.5:
            raise ValueError(f"Validation failed for '{final_table_name_project_repos_contributors}': Record count deviation ({deviation:.1%}) exceeds 50%.")
        context.log.info(f"Record count deviation for '{final_table_name_project_repos_contributors}' ({deviation:.1%}) within 50% threshold.")
    elif new_project_repos_contributors_count > 0:
        context.log.info(f"Target table '{final_table_name_project_repos_contributors}' had 0 records, new data has records. This is acceptable.")
    else:
        raise ValueError(f"Validation failed for '{final_table_name_project_repos_contributors}': Both existing and new data appear empty (0 records).")

    context.log.info("All validations passed.")
    return True

# to accomodate multiple environments, we will use a factory function
def create_process_compressed_contributors_data_asset(env_prefix: str):

    # Construct the AssetKey for the upstream dependency.
    # The upstream asset's base name is "github_project_repos_contributors".
    # It will also have the same env_prefix.
    upstream_dependency_key = AssetKey([env_prefix, "github_project_repos_contributors"])

    @dg.asset(
        key_prefix=env_prefix,
        name="process_compressed_contributors_data",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"},
        group_name="clean_data",
        tags={
            "contributor_lock": "True"
        },
        deps=[upstream_dependency_key],
        automation_condition=dg.AutomationCondition.eager(),
    )
    def _process_compressed_contributors_data_env_specific(context) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        clean_schema = env_config["clean_schema"]
        raw_schema = env_config["raw_schema"]

        # Define table names
        staging_table_name_contributors = "latest_contributors_staging"
        final_table_name_contributors = "latest_contributors"
        old_table_name_contributors = "latest_contributors_old"
        staging_table_name_project_repos_contributors = "latest_project_repos_contributors_staging"
        final_table_name_project_repos_contributors = "latest_project_repos_contributors"
        old_table_name_project_repos_contributors = "latest_project_repos_contributors_old"
        
        # A temporary table to hold all contributor records before deduplication
        # temp_staging_all_contributors = "latest_contributors_all_temp"
        # temp_staging_all_project_repos_contributors = "latest_project_repos_contributors_all_temp"
        # A temporary table to hold only the NEWLY processed contributor records
        temp_new_contributors_table = "temp_newly_processed_contributors"

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        # Execute the query
        # Extracts, decompresses, and inserts data into the clean table.
        try:
            with cloud_sql_engine.connect() as conn:
                with conn.begin():

                    # first get data_timestamp from source and target tables to compare
                    result = conn.execute(text(f"select max(data_timestamp) from {raw_schema}.project_repos_contributors"))
                    max_source_ts_aware = result.scalar()
                    result = conn.execute(text(f"select max(data_timestamp) from {clean_schema}.latest_contributors"))
                    target_ts_aware_contributors = result.scalar()

                    # confirm max_source_ts_aware and target_ts_aware_contributors are not None
                    if max_source_ts_aware is None or target_ts_aware_contributors is None:
                        raise ValueError("Validation failed: max_source_ts_aware or target_ts_aware_contributors is None.")

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

                    # set the data_timestamp up front so it doesn't change with batching
                    # data_timestamp = pd.Timestamp.now()
                    data_timestamp = max_source_ts_aware

                    # --- Setup Staging Tables ---
                    context.log.info("Dropping old staging tables if they exist...")
                
                    conn.execute(text(f"DROP TABLE IF EXISTS {raw_schema}.{staging_table_name_contributors} CASCADE;"))
                    conn.execute(text(f"DROP TABLE IF EXISTS {raw_schema}.{staging_table_name_project_repos_contributors} CASCADE;"))
                    # conn.execute(text(f"DROP TABLE IF EXISTS {raw_schema}.{temp_staging_all_contributors} CASCADE;"))
                    # conn.execute(text(f"DROP TABLE IF EXISTS {raw_schema}.{temp_staging_all_project_repos_contributors} CASCADE;"))

                    # --- Generator for Streaming Data ---
                    def generate_contributor_rows(db_connection):
                        context.log.info("Streaming source data from the database...")
                        source_query = text(f"""
                            SELECT repo, contributor_list
                            FROM {raw_schema}.project_repos_contributors
                            WHERE data_timestamp = (SELECT MAX(data_timestamp) FROM {raw_schema}.project_repos_contributors)
                        """)
                        
                        # Iterate directly on the result proxy to avoid loading all rows into memory
                        for repo, compressed_byte_data in db_connection.execute(source_query):
                            if not compressed_byte_data:
                                continue
                            try:
                                # Decompress and parse the data
                                contributors_json_string = gzip.decompress(compressed_byte_data)
                                contributors_list = json.loads(contributors_json_string)
                                
                                # Yield one dictionary per contributor
                                for contributor in contributors_list:
                                    if contributor.get('type') != 'Anonymous':
                                        yield {
                                            "repo": repo,
                                            "contributor_login": contributor.get('login'),
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
                                            "contributor_contributions": contributor.get('contributions'),
                                            "contributor_name": contributor.get('name'), # contributor.get('name') is not always present
                                            "contributor_email": contributor.get('email'), # contributor.get('email') is not always present,
                                            "contributor_unique_id_builder_love": f"{contributor.get('login')}|{contributor.get('id')}", # derived unique identifier for the contributor
                                        }
                                    else: # Handle Anonymous contributors
                                        yield {
                                            "repo": repo,
                                            "contributor_login": contributor.get('name'),
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
                                            "contributor_contributions": contributor.get('contributions'),
                                            "contributor_name": contributor.get('name'), # contributor.get('name') is not always present
                                            "contributor_email": contributor.get('email'), # contributor.get('email') is not always present,
                                            "contributor_unique_id_builder_love": f"{contributor.get('name')}|{contributor.get('email')}",
                                        }
                            except (gzip.BadGzipFile, json.JSONDecodeError) as e:
                                context.log.warning(f"Skipping repo '{repo}' due to processing error: {e}")
                                continue

                    # --- Process and Load NEW Data into a Temporary Table in RAW schema ---
                    context.log.info(f"Processing new data and loading to temporary table '{raw_schema}.{temp_new_contributors_table}'...")
                    chunk_size = 10000
                    data_chunk = []

                    temp_table_columns = [
                        'repo', 'contributor_login', 'contributor_id', 'contributor_node_id', 
                        'contributor_avatar_url', 'contributor_gravatar_id', 'contributor_url', 
                        'contributor_html_url', 'contributor_followers_url', 'contributor_following_url', 
                        'contributor_gists_url', 'contributor_starred_url', 'contributor_subscriptions_url', 
                        'contributor_organizations_url', 'contributor_repos_url', 'contributor_events_url', 
                        'contributor_received_events_url', 'contributor_type', 'contributor_user_view_type', 
                        'contributor_contributions', 'contributor_name', 'contributor_email', 
                        'contributor_unique_id_builder_love', 'data_timestamp'
                    ]

                    for row_dict in generate_contributor_rows(conn):
                        data_chunk.append(row_dict)
                        if len(data_chunk) >= chunk_size:
                            chunk_df = pd.DataFrame(data_chunk).reindex(columns=temp_table_columns)
                            chunk_df['data_timestamp'] = data_timestamp
                            chunk_df.to_sql(temp_new_contributors_table, conn, schema=raw_schema, if_exists='append', index=False)
                            data_chunk = []

                    if data_chunk:
                        chunk_df = pd.DataFrame(data_chunk).reindex(columns=temp_table_columns)
                        chunk_df['data_timestamp'] = data_timestamp
                        chunk_df.to_sql(temp_new_contributors_table, conn, schema=raw_schema, if_exists='append', index=False)

                    # --- Combine New and Existing Data from RAW schema ---
                    context.log.info("Combining new and existing data from RAW schema into final staging tables...")
                    inspector = inspect(conn)
                    final_contributors_exists = inspector.has_table(final_table_name_contributors, schema=raw_schema)
                    final_project_repos_exists = inspector.has_table(final_table_name_project_repos_contributors, schema=raw_schema)

                    # For latest_contributors
                    combine_sql_contributors = f"""
                        CREATE TABLE {raw_schema}.{staging_table_name_contributors} AS
                        WITH combined_data AS (
                            SELECT 
                                contributor_unique_id_builder_love,
                                contributor_login,
                                contributor_id,
                                contributor_node_id,
                                contributor_avatar_url,
                                contributor_gravatar_id,
                                contributor_url,
                                contributor_html_url,
                                contributor_followers_url,
                                contributor_following_url,
                                contributor_gists_url,
                                contributor_starred_url,
                                contributor_subscriptions_url,
                                contributor_organizations_url,
                                contributor_repos_url,
                                contributor_events_url,
                                contributor_received_events_url,
                                contributor_type,
                                contributor_user_view_type,
                                contributor_name,
                                contributor_email,
                                data_timestamp,
                            1 as priority FROM {raw_schema}.{temp_new_contributors_table}
                            {'UNION ALL' if final_contributors_exists else ''}
                            {'SELECT *, 2 as priority FROM ' + raw_schema + '.' + final_table_name_contributors if final_contributors_exists else ''}
                        ),
                        ranked_data AS (
                            SELECT *, ROW_NUMBER() OVER(PARTITION BY contributor_unique_id_builder_love ORDER BY priority ASC, data_timestamp DESC) as rn
                            FROM combined_data
                        )
                        SELECT 
                            contributor_unique_id_builder_love,
                            contributor_login,
                            contributor_id,
                            contributor_node_id,
                            contributor_avatar_url,
                            contributor_gravatar_id,
                            contributor_url,
                            contributor_html_url,
                            contributor_followers_url,
                            contributor_following_url,
                            contributor_gists_url,
                            contributor_starred_url,
                            contributor_subscriptions_url,
                            contributor_organizations_url,
                            contributor_repos_url,
                            contributor_events_url,
                            contributor_received_events_url,
                            contributor_type,
                            contributor_user_view_type,
                            contributor_name,
                            contributor_email,
                            data_timestamp
                        FROM ranked_data WHERE rn = 1;
                    """
                    conn.execute(text(combine_sql_contributors))

                    # For latest_project_repos_contributors
                    combine_sql_project_repos = f"""
                        CREATE TABLE {raw_schema}.{staging_table_name_project_repos_contributors} AS
                        WITH combined_data AS (
                            SELECT contributor_unique_id_builder_love, repo, contributor_contributions, data_timestamp, 1 as priority
                            FROM {raw_schema}.{temp_new_contributors_table}
                            {'UNION ALL' if final_project_repos_exists else ''}
                            {'SELECT contributor_unique_id_builder_love, repo, contributor_contributions, data_timestamp, 2 as priority FROM ' + raw_schema + '.' + final_table_name_project_repos_contributors if final_project_repos_exists else ''}
                        ),
                        ranked_data AS (
                            SELECT *, ROW_NUMBER() OVER(PARTITION BY contributor_unique_id_builder_love, repo ORDER BY priority ASC, data_timestamp DESC) as rn
                            FROM combined_data
                        )
                        SELECT contributor_unique_id_builder_love, repo, contributor_contributions, data_timestamp
                        FROM ranked_data WHERE rn = 1;
                    """
                    conn.execute(text(combine_sql_project_repos))

                    conn.execute(text(f"DROP TABLE {raw_schema}.{temp_new_contributors_table};"))

                # --- Validate Using Staging Table Counts against existing clean schema table ---
                new_count_contributors = conn.execute(text(f"SELECT COUNT(*) FROM {raw_schema}.{staging_table_name_contributors}")).scalar()
                new_count_project_repos = conn.execute(text(f"SELECT COUNT(*) FROM {raw_schema}.{staging_table_name_project_repos_contributors}")).scalar()
                
                run_validations(
                    context=context,
                    new_contributors_count=new_count_contributors,
                    new_project_repos_contributors_count=new_count_project_repos,
                    engine=cloud_sql_engine,
                    target_schema=clean_schema # Validate against the tables in the clean schema
                )

                context.log.info("All validations passed. Proceeding with atomic swap...")

            # Perform Atomic Swap via Transaction
            context.log.info(f"Performing atomic swap to update {raw_schema}.{final_table_name_contributors} and {raw_schema}.{final_table_name_project_repos_contributors}...")
            with cloud_sql_engine.connect() as conn:
                with conn.begin(): # Start transaction
                    # Use CASCADE if Foreign Keys might point to the table
                    context.log.info(f"Dropping old tables {raw_schema}.{old_table_name_contributors} and {raw_schema}.{old_table_name_project_repos_contributors} if they exist...")
                    conn.execute(text(f"DROP TABLE IF EXISTS {raw_schema}.{old_table_name_contributors} CASCADE;"))
                    conn.execute(text(f"DROP TABLE IF EXISTS {raw_schema}.{old_table_name_project_repos_contributors} CASCADE;"))

                    context.log.info(f"Renaming current {raw_schema}.{final_table_name_contributors} to {raw_schema}.{old_table_name_contributors} (if it exists)...")
                    conn.execute(text(f"ALTER TABLE IF EXISTS {raw_schema}.{final_table_name_contributors} RENAME TO {old_table_name_contributors};"))
                    context.log.info(f"Renaming current {raw_schema}.{final_table_name_project_repos_contributors} to {raw_schema}.{old_table_name_project_repos_contributors} (if it exists)...")
                    conn.execute(text(f"ALTER TABLE IF EXISTS {raw_schema}.{final_table_name_project_repos_contributors} RENAME TO {old_table_name_project_repos_contributors};"))

                    context.log.info(f"Renaming staging tables {raw_schema}.{staging_table_name_contributors} and {raw_schema}.{staging_table_name_project_repos_contributors} to {raw_schema}.{final_table_name_contributors} and {raw_schema}.{final_table_name_project_repos_contributors}...")
                    conn.execute(text(f"ALTER TABLE {raw_schema}.{staging_table_name_contributors} RENAME TO {final_table_name_contributors};"))
                    conn.execute(text(f"ALTER TABLE {raw_schema}.{staging_table_name_project_repos_contributors} RENAME TO {final_table_name_project_repos_contributors};"))
                # Transaction commits here if no exceptions were raised inside the 'with conn.begin()' block
            context.log.info("Atomic swap successful.")

            # cleanup old table (outside the main transaction)
            context.log.info(f"Cleaning up old tables {raw_schema}.{old_table_name_contributors} and {raw_schema}.{old_table_name_project_repos_contributors}...")
            try:
                with cloud_sql_engine.connect() as conn:
                    with conn.begin():
                        conn.execute(text(f"DROP TABLE IF EXISTS {raw_schema}.{old_table_name_contributors} CASCADE;"))
                        conn.execute(text(f"DROP TABLE IF EXISTS {raw_schema}.{old_table_name_project_repos_contributors} CASCADE;"))
                        context.log.info(f"Cleaned up tables {raw_schema}.{old_table_name_contributors} and {raw_schema}.{old_table_name_project_repos_contributors}.")
            except Exception as cleanup_e:
                # Log warning - cleanup failure shouldn't fail the asset run
                context.log.warning(f"Could not drop old tables {raw_schema}.{old_table_name_contributors} and {raw_schema}.{old_table_name_project_repos_contributors}: {cleanup_e}")

            # recreate the indexes for both tables
            try:
                with cloud_sql_engine.connect() as conn:
                    with conn.begin():
                        # create the unique_id index
                        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_latest_contributors_unique_id ON {raw_schema}.{final_table_name_contributors} (contributor_unique_id_builder_love);"))
                        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_latest_project_repos_contributors_unique_id ON {raw_schema}.{final_table_name_project_repos_contributors} (contributor_unique_id_builder_love);"))

                        # create repo index
                        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_latest_project_repos_contributors_repo ON {raw_schema}.{final_table_name_project_repos_contributors} (repo);"))

                        # analyze the tables to ensure the indexes are used
                        conn.execute(text(f"ANALYZE {raw_schema}.{final_table_name_contributors};"))
                        conn.execute(text(f"ANALYZE {raw_schema}.{final_table_name_project_repos_contributors};"))
            except Exception as index_e:
                context.log.warning(f"Could not create indexes for {raw_schema}.{final_table_name_contributors} and {raw_schema}.{final_table_name_project_repos_contributors}: {index_e}")
                raise e

            # Fetch Metadata from the FINAL table for MaterializeResult
            context.log.info("Fetching metadata for Dagster result...")
            with cloud_sql_engine.connect() as conn:
                row_count_result = conn.execute(text(f"SELECT COUNT(*) FROM {raw_schema}.{final_table_name_contributors}"))
                row_count_result_project_repos_contributors = conn.execute(text(f"SELECT COUNT(*) FROM {raw_schema}.{final_table_name_project_repos_contributors}"))
                # Use scalar_one() for single value, assumes table not empty after swap
                row_count = row_count_result.scalar_one()
                row_count_project_repos_contributors = row_count_result_project_repos_contributors.scalar_one()

                preview_result = conn.execute(text(f"SELECT * FROM {raw_schema}.{final_table_name_contributors} LIMIT 10"))
                preview_result_project_repos_contributors = conn.execute(text(f"SELECT * FROM {raw_schema}.{final_table_name_project_repos_contributors} LIMIT 10"))
                # Fetch into dicts using .mappings().all() for easy DataFrame creation
                result_df = pd.DataFrame(preview_result.mappings().all())
                result_df_project_repos_contributors = pd.DataFrame(preview_result_project_repos_contributors.mappings().all())

            context.log.info(f"Asset materialization complete. Final row count: {row_count}")
            context.log.info(f"Asset materialization complete. Final row count: {row_count_project_repos_contributors}")
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

    # return empty config
    return _process_compressed_contributors_data_env_specific

########################################################################################################################


########################################################################################################################
# crypto ecosystems raw file assets
########################################################################################################################

@op(
    required_resource_keys={"dbt_cli"},
    out={"dbt_tests_passed": Out(bool)}
)
def run_dbt_tests_on_crypto_ecosystems_raw_file_staging(context) -> bool:
    dbt = context.resources.dbt_cli
    target_name = dbt.target
    context.log.info(f"----------************* Running in environment: {target_name} *************----------")
    context.log.info(f"Running dbt tests on path:tests/crypto_ecosystems/ for dbt target: {target_name}.")

    invocation_obj = None
    try:
        invocation_obj = dbt.cli(
            ["test", "--select", "path:tests/crypto_ecosystems/"], context=context
        )
        # The .wait() method from dagster-dbt is designed to raise an exception
        # if the dbt command itself returns a non-zero exit code (i.e., fails).
        invocation_obj.wait()

        context.log.info(f"dbt tests on path:tests/crypto_ecosystems/ (target: {target_name}) reported success via CLI exit code.")
        yield Output(True, output_name="dbt_tests_passed")
        return True

    except Exception as e: # Catch ANY exception from the .cli() or .wait() calls
        context.log.error(
            f"dbt test op for target {target_name} encountered an error: {str(e)}",
            exc_info=True # This will log the full traceback of 'e'
        )
        
        # Attempt to get dbt stdout if the exception 'e' might be a DbtCliTaskFailureError
        # DbtCliTaskFailureError (even if we can't import its type) often has an 'invocation' attribute.
        stdout_log = "N/A"
        if hasattr(e, 'invocation') and e.invocation and hasattr(e.invocation, 'get_stdout'):
            stdout_log = e.invocation.get_stdout()
            context.log.info(f"dbt stdout from failure:\n{stdout_log}")
        else:
            context.log.info(f"Could not retrieve specific dbt stdout from exception of type {type(e)}.")

        # Raise a new, generic Exception to ensure the Dagster op fails
        # This will stop the Dagster job.
        raise Exception(
            f"dbt test operation failed for target {target_name}. Original error type: {type(e).__name__}. Message: {str(e)}. See logs for details and dbt stdout if available."
        ) from e # Chain the original exception for better traceback

# perform the DML work
# load new data from crypto_ecosystems_raw_file_staging table into crypto_ecosystems_raw_file table, and archive any data that is not in staging table
@op(
    required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"}
)
def load_new_data_from_staging_to_final(context, previous_op_result):
    """
    Loads new data from the environment-specific staging table 
    (e.g., raw.crypto_ecosystems_raw_file_staging or raw_stg.crypto_ecosystems_raw_file_staging)
    into the environment-specific final table (e.g., raw.crypto_ecosystems_raw_file or raw_stg.crypto_ecosystems_raw_file),
    and archives data from the final table that is not present in the staging table.
    """
    env_config = context.resources.active_env_config
    # Determine schema names based on the active environment configuration
    # Assuming 'raw_schema' is defined in your active_env_config_resource
    # to be "raw" for prod and "raw_stg" for staging.
    raw_schema = env_config["raw_schema"] 

    # tell the user what environment we are running in
    context.log.info(f"----------************* Running in environment: {env_config['env']} *************----------")

    main_table = f"{raw_schema}.crypto_ecosystems_raw_file"
    staging_table = f"{raw_schema}.crypto_ecosystems_raw_file_staging"
    archive_table = f"{raw_schema}.crypto_ecosystems_raw_file_archive"

    context.log.info(f"Operating on main table: {main_table}, staging table: {staging_table}, archive table: {archive_table}")

    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        dml_query = text(f"""
            -- Step 1a: Identify the primary keys (or ctids) of rows to delete ONCE
            CREATE TEMP TABLE IF NOT EXISTS tmp_keys_to_delete AS
            SELECT main.id 
            FROM {main_table} main
            LEFT JOIN {staging_table} sk
                ON main.project_title = sk.project_title
                AND main.sub_ecosystems IS NOT DISTINCT FROM sk.sub_ecosystems
                AND main.repo = sk.repo
                AND main.tags IS NOT DISTINCT FROM sk.tags
            WHERE sk.repo IS NULL;

            -- Step 1b: Archive rows by joining main table to the keys identified
            INSERT INTO {archive_table} (
                id, project_title, sub_ecosystems, repo, tags, data_timestamp,
                archived_at
            )
            SELECT
                main.id, main.project_title, main.sub_ecosystems, main.repo, main.tags, main.data_timestamp,
                NOW() AT TIME ZONE 'utc'
            FROM {main_table} main
            JOIN tmp_keys_to_delete keys_td ON main.id = keys_td.id; -- Join using the key

            -- Step 1c: Delete rows from main using the identified keys (MUCH more efficient)
            DELETE FROM {main_table} main
            WHERE main.id IN (SELECT id FROM tmp_keys_to_delete); -- Use simple IN clause with keys

            -- Step 1d: Clean up the temporary table
            DROP TABLE tmp_keys_to_delete;

            -- 2. Insert new rows (those in staging table but not having an exact match in main table)
            INSERT INTO {main_table} (
                project_title, sub_ecosystems, repo, tags, data_timestamp -- list all columns
            )
            SELECT
                stg.project_title, stg.sub_ecosystems, stg.repo, stg.tags, stg.data_timestamp -- select all columns
            FROM {staging_table} stg
            LEFT JOIN {main_table} main
                ON stg.project_title = main.project_title
                AND stg.sub_ecosystems IS NOT DISTINCT FROM main.sub_ecosystems
                AND stg.repo = main.repo
                AND stg.tags IS NOT DISTINCT FROM main.tags
            WHERE main.repo IS NULL; -- If any part of the composite key in main is NULL, it means no match was found
        """)
        conn.execute(dml_query)
        conn.commit()
    context.log.info(
        f"Data updates from {staging_table} loaded to {main_table}. "
        f"Non-matching data from {main_table} archived to {archive_table}."
    )
    yield Output(None) # Use yield

######################################
# maintain 'project' dimension records
######################################

@op(
    required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"},
    ins={"previous_op_result": In(Nothing)}, # Ensures it runs after the previous op
)
def update_projects_dimension_and_archive(context):
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    env_config = context.resources.active_env_config
    
    # Determine schema for target tables. Adjust if 'projects' tables are in a different schema.
    # For this example, assuming they are in the same schema as the raw table.
    target_schema = env_config["raw_schema"] 
    
    projects_table = f"{target_schema}.projects"
    projects_archive_table = f"{target_schema}.projects_archive"
    source_table = f"{env_config['raw_schema']}.crypto_ecosystems_raw_file" # Source of project_titles

    sql_now = "NOW() AT TIME ZONE 'utc'" # Consistent timestamping

    with cloud_sql_engine.connect() as conn:
        with conn.begin(): # Start transaction
            context.log.info(f"Updating {projects_table} and {projects_archive_table} from {source_table}...")

            # Step 1: Create a temporary table of distinct, non-null project_titles currently in the source
            conn.execute(text(f"""
                CREATE TEMP TABLE tmp_current_active_project_titles AS
                SELECT DISTINCT project_title
                FROM {source_table}
                WHERE project_title IS NOT NULL;
            """))
            context.log.info("Temporary table tmp_current_active_project_titles created.")

            # Step 2: Upsert into the 'projects' table.
            # - New project_titles are inserted. 'project_id' is auto-generated by BIGSERIAL.
            #   'first_seen_timestamp' defaults to NOW() or is set here. 'last_seen_timestamp' is set. 'is_active' is TRUE.
            # - Existing project_titles found in the source will have their 'last_seen_timestamp' updated
            #   and 'is_active' set to TRUE (in case they were previously inactive).
            #   'first_seen_timestamp' for existing projects is NOT updated by the ON CONFLICT clause.
            upsert_sql = f"""
                INSERT INTO {projects_table} (project_title, first_seen_timestamp, last_seen_timestamp, is_active)
                SELECT
                    capt.project_title,
                    {sql_now}, -- This will be the first_seen_timestamp for NEW projects
                    {sql_now}, -- last_seen_timestamp for new projects and those being updated
                    TRUE      -- is_active for new projects and those being updated
                FROM tmp_current_active_project_titles capt
                ON CONFLICT (project_title) DO UPDATE
                SET
                    last_seen_timestamp = {sql_now},
                    is_active = TRUE
                WHERE {projects_table}.project_title = EXCLUDED.project_title; 
                -- The WHERE clause in DO UPDATE is technically not needed if project_title is the conflict target
                -- but can be kept for clarity or complex conditions.
            """
            result = conn.execute(text(upsert_sql))
            context.log.info(f"{result.rowcount} rows affected by upsert into {projects_table}.")


            # Step 3: Identify projects that were active but are no longer in the source table.
            # These are candidates for archiving.
            conn.execute(text(f"""
                CREATE TEMP TABLE tmp_projects_to_archive AS
                SELECT p.project_id, p.project_title, p.first_seen_timestamp, p.last_seen_timestamp AS last_seen_timestamp_while_active
                FROM {projects_table} p
                LEFT JOIN tmp_current_active_project_titles capt ON p.project_title = capt.project_title
                WHERE p.is_active = TRUE AND capt.project_title IS NULL;
            """))
            context.log.info("Temporary table tmp_projects_to_archive created.")

            # Step 4: Insert these projects into the 'projects_archive' table.
            archive_sql = f"""
                INSERT INTO {projects_archive_table} (project_id, project_title, first_seen_timestamp, last_seen_timestamp_while_active, archived_at)
                SELECT
                    pta.project_id,
                    pta.project_title,
                    pta.first_seen_timestamp,
                    pta.last_seen_timestamp_while_active,
                    {sql_now}
                FROM tmp_projects_to_archive pta;
            """
            result = conn.execute(text(archive_sql))
            context.log.info(f"{result.rowcount} projects moved to {projects_archive_table}.")

            # Step 5: Mark the archived projects as inactive in the 'projects' table.
            deactivate_sql = f"""
                UPDATE {projects_table} p
                SET is_active = FALSE
                FROM tmp_projects_to_archive pta
                WHERE p.project_id = pta.project_id;
            """
            result = conn.execute(text(deactivate_sql))
            context.log.info(f"{result.rowcount} projects marked as inactive in {projects_table}.")

            # Step 6: Clean up temporary tables
            conn.execute(text("DROP TABLE IF EXISTS tmp_current_active_project_titles;"))
            conn.execute(text("DROP TABLE IF EXISTS tmp_projects_to_archive;"))
            context.log.info("Temporary tables dropped.")

        # Transaction commits here if no exceptions were raised

    # Materialize an asset event with metadata
    # Fetch some counts or previews for metadata
    with cloud_sql_engine.connect() as conn:
        active_projects_count = conn.execute(text(f"SELECT COUNT(*) FROM {projects_table} WHERE is_active = TRUE;")).scalar_one()
        archived_projects_count_total = conn.execute(text(f"SELECT COUNT(*) FROM {projects_archive_table};")).scalar_one()
        
        preview_df = pd.read_sql(f"SELECT * FROM {projects_table} ORDER BY last_seen_timestamp DESC NULLS LAST LIMIT 5", conn)

    yield AssetMaterialization(
        asset_key=AssetKey(["crypto_ecosystems", "projects_dimension"]),
        description="Dimension table of unique project titles and their active status.",
        metadata={
            "active_projects_count": MetadataValue.int(active_projects_count),
            "total_archived_projects_count": MetadataValue.int(archived_projects_count_total),
            "preview": MetadataValue.md(preview_df.to_markdown(index=False) if not preview_df.empty else "No active projects found for preview."),
            "projects_table_name": projects_table,
            "projects_archive_table_name": projects_archive_table
        }
    )
    yield Output(None)


######################################
# maintain 'repos' dimension records
######################################

@op(
    required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"},
    ins={"after_projects_updated": In(Nothing)}, # Name this input to reflect dependency
)
def update_repos_dimension_and_archive(context): # Use the input name
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    env_config = context.resources.active_env_config
    
    target_schema = env_config["raw_schema"]  # Or your 'derived' schema
    
    repos_table = f"{target_schema}.repos"
    repos_archive_table = f"{target_schema}.repos_archive"
    source_table = f"{env_config['raw_schema']}.crypto_ecosystems_raw_file" # Source of repo URLs/identifiers

    sql_now = "NOW() AT TIME ZONE 'utc'" # Consistent timestamping

    with cloud_sql_engine.connect() as conn:
        with conn.begin(): # Start transaction
            context.log.info(f"Updating {repos_table} and {repos_archive_table} from {source_table}...")

            # Step 1: Create a temporary table of distinct, non-null repo URLs/identifiers currently in the source
            conn.execute(text(f"""
                CREATE TEMP TABLE tmp_current_active_repos AS
                SELECT DISTINCT repo -- Use 'repo' to match target table column
                FROM {source_table}
                WHERE repo IS NOT NULL;
            """))
            context.log.info("Temporary table tmp_current_active_repos created.")

            # Step 2: Upsert into the 'repos' table.
            upsert_sql = f"""
                INSERT INTO {repos_table} (repo, first_seen_timestamp, last_seen_timestamp, is_active)
                SELECT
                    car.repo,
                    {sql_now}, -- This will be the first_seen_timestamp for NEW repos
                    {sql_now}, -- last_seen_timestamp for new repos and those being updated
                    TRUE      -- is_active for new repos and those being updated
                FROM tmp_current_active_repos car
                ON CONFLICT (repo) DO UPDATE
                SET
                    last_seen_timestamp = {sql_now},
                    is_active = TRUE;
                -- No WHERE clause needed in DO UPDATE if conflict target is the only condition
            """
            result = conn.execute(text(upsert_sql))
            context.log.info(f"{result.rowcount} rows affected by upsert into {repos_table}.")

            # Step 3: Identify repos that were active but are no longer in the source table.
            conn.execute(text(f"""
                CREATE TEMP TABLE tmp_repos_to_archive AS
                SELECT r.repo_id, r.repo, r.first_seen_timestamp, r.last_seen_timestamp AS last_seen_timestamp_while_active
                FROM {repos_table} r
                LEFT JOIN tmp_current_active_repos car ON r.repo = car.repo
                WHERE r.is_active = TRUE AND car.repo IS NULL;
            """))
            context.log.info("Temporary table tmp_repos_to_archive created.")

            # Step 4: Insert these repos into the 'repos_archive' table.
            archive_sql = f"""
                INSERT INTO {repos_archive_table} (repo_id, repo, first_seen_timestamp, last_seen_timestamp_while_active, archived_at)
                SELECT
                    rta.repo_id,
                    rta.repo,
                    rta.first_seen_timestamp,
                    rta.last_seen_timestamp_while_active,
                    {sql_now}
                FROM tmp_repos_to_archive rta;
            """
            result = conn.execute(text(archive_sql))
            context.log.info(f"{result.rowcount} repos moved to {repos_archive_table}.")

            # Step 5: Mark the archived repos as inactive in the 'repos' table.
            deactivate_sql = f"""
                UPDATE {repos_table} r
                SET is_active = FALSE
                FROM tmp_repos_to_archive rta
                WHERE r.repo_id = rta.repo_id;
            """
            result = conn.execute(text(deactivate_sql))
            context.log.info(f"{result.rowcount} repos marked as inactive in {repos_table}.")

            # Step 6: Clean up temporary tables
            conn.execute(text("DROP TABLE IF EXISTS tmp_current_active_repos;"))
            conn.execute(text("DROP TABLE IF EXISTS tmp_repos_to_archive;"))
            context.log.info("Temporary tables dropped.")

        # Transaction commits here if no exceptions were raised

    # Materialize an asset event with metadata
    with cloud_sql_engine.connect() as conn:
        active_repos_count = conn.execute(text(f"SELECT COUNT(*) FROM {repos_table} WHERE is_active = TRUE;")).scalar_one()
        archived_repos_count_total = conn.execute(text(f"SELECT COUNT(*) FROM {repos_archive_table};")).scalar_one()
        
        preview_df = pd.read_sql(f"SELECT * FROM {repos_table} ORDER BY last_seen_timestamp DESC NULLS LAST LIMIT 5", conn)

    yield AssetMaterialization(
        asset_key=AssetKey(["crypto_ecosystems", "repos_dimension"]), # New asset key
        description="Dimension table of unique repository URLs/identifiers and their active status.",
        metadata={
            "active_repos_count": MetadataValue.int(active_repos_count),
            "total_archived_repos_count": MetadataValue.int(archived_repos_count_total),
            "preview": MetadataValue.md(preview_df.to_markdown(index=False) if not preview_df.empty else "No active repos found for preview."),
            "repos_table_name": repos_table,
            "repos_archive_table_name": repos_archive_table
        }
    )
    yield Output(None)