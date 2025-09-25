import dagster as dg
from dagster import AssetIn, AssetKey, Config
import os
import io
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from urllib.parse import urlparse
import pandas as pd
from sqlalchemy import text, create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy
import re
import time
import random
import gzip
import json
import numpy as np
import traceback
from .resources import github_api_resource 
import psutil
from datetime import datetime, timedelta, timezone

# Define the config schema for the github api resource
class GithubAssetConfig(Config):
    key_name: str

########################################################################################################################
# lookup and swap github legacy contributor node id for the new format contributor node id
########################################################################################################################

def contributor_node_id_swap(context, df_contributors: pd.DataFrame, cloud_sql_engine) -> pd.DataFrame:
    """
    Swaps legacy GitHub contributor node IDs in a DataFrame for the new format IDs
    by looking up values in the clean.latest_contributor_data table.

    Args:
        context: The Dagster context object (for logging).
        df_contributors: Input DataFrame with a 'contributor_node_id' column
                         that might contain legacy IDs.
        cloud_sql_engine: SQLAlchemy engine connected to the database.

    Returns:
        A pandas DataFrame with the 'contributor_node_id' column updated
        to the new format where applicable.
    """
    context.log.info("Starting contributor node ID swap process.")
    env_config = context.resources.active_env_config  # Get environment config
    clean_schema = env_config["clean_schema"]          # Get dynamic clean schema name

    # --- Input Validation ---
    if 'contributor_node_id' not in df_contributors.columns:
         context.log.error("Input dataframe df_contributors is missing the 'contributor_node_id' column. Skipping swap.")
         return df_contributors

    if df_contributors.empty:
        context.log.info("Input dataframe df_contributors is empty. Skipping swap.")
        return df_contributors

    # --- Read Lookup Data ---
    try:
        # Construct the SQL query using sqlalchemy.text to handle potential schema names safely
        lookup_query = text(f"""
            SELECT DISTINCT
                contributor_node_id,       -- New format ID
                contributor_node_id_legacy -- Legacy format ID
            FROM {clean_schema}.latest_contributor_data
            WHERE contributor_node_id_legacy IS NOT NULL
              AND contributor_node_id IS NOT NULL
        """)
        # Read only necessary columns and distinct pairs
        df_lookup = pd.read_sql(lookup_query, cloud_sql_engine)

        context.log.info(f"Successfully read {len(df_lookup)} distinct lookup rows from {clean_schema}.latest_contributor_data.")

        # Handle case where lookup table is empty or missing columns (though query selects them)
        if df_lookup.empty:
            context.log.warning(f"Lookup table {clean_schema}.latest_contributor_data returned no data or no relevant pairs. Skipping swap.")
            return df_contributors
        if 'contributor_node_id' not in df_lookup.columns or 'contributor_node_id_legacy' not in df_lookup.columns:
             context.log.error(f"Lookup table {clean_schema}.latest_contributor_data is missing required columns after query. Skipping swap.")
             return df_contributors

    except Exception as e:
        context.log.error(f"Failed to read from {clean_schema}.latest_contributor_data: {e}. Skipping swap.")
        # Depending on requirements, you might want to raise the exception instead
        # raise e
        return df_contributors

    # --- Perform the Swap using Merge ---

    # Rename the new ID column in the lookup table to avoid naming conflicts after merge
    df_lookup = df_lookup.rename(columns={'contributor_node_id': 'contributor_node_id_new'})

    # Perform a left merge: Keep all rows from df_contributors.
    # Match df_contributors.contributor_node_id (potentially legacy) with df_lookup.contributor_node_id_legacy
    context.log.info(f"Performing left merge on 'contributor_node_id' (activity) == 'contributor_node_id_legacy' (lookup). Input df shape: {df_contributors.shape}")
    df_merged = pd.merge(
        df_contributors,
        df_lookup[['contributor_node_id_new', 'contributor_node_id_legacy']], # Only merge necessary lookup columns
        how='left',
        left_on='contributor_node_id',
        right_on='contributor_node_id_legacy'
    )
    context.log.info(f"Merge complete. Merged DataFrame shape: {df_merged.shape}")

    # Identify rows where a swap should occur (a new ID was found via the legacy link)
    swap_condition = df_merged['contributor_node_id_new'].notna()
    num_swapped = swap_condition.sum()
    context.log.info(f"Found {num_swapped} rows where contributor_node_id can be updated.")

    # Update the original 'contributor_node_id' column
    # If contributor_node_id_new is not null (match found), use it. Otherwise, keep the original.
    df_merged['contributor_node_id'] = np.where(
        swap_condition,                          # Condition: New ID found?
        df_merged['contributor_node_id_new'],    # Value if True: Use the new ID
        df_merged['contributor_node_id']         # Value if False: Keep original ID
    )

    # --- Cleanup and Return ---
    # Drop the temporary columns added by the merge
    # Use errors='ignore' in case the columns weren't added (e.g., if lookup was empty)
    df_result = df_merged.drop(columns=['contributor_node_id_new', 'contributor_node_id_legacy'], errors='ignore')

    context.log.info(f"Contributor node ID swap process completed. Final DataFrame shape: {df_result.shape}")

    return df_result



# Helper function to get the export.jsonl file from GCS. 
# uses io_manager to get the filepath output by the upstream asset
# Handles streaming, batching, and writing to Postgres.
def stream_gcs_to_postgres_in_batches(context, gcs_path: str, table_name: str, schema: str, batch_size: int = 10000, data_timestamp: datetime = None):
    """
    Streams a JSONL file from GCS, processes it in batches, and appends each batch
    to a PostgreSQL table. 
    """
    gcs_resource = context.resources.gcs
    storage_client = gcs_resource.get_client()
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # 1. Parse GCS Path
    try:
        parsed_path = urlparse(gcs_path)
        bucket_name = parsed_path.netloc
        blob_name = parsed_path.path.lstrip('/')
    except Exception as e:
        raise ValueError(f"Could not parse GCS path '{gcs_path}': {e}")

    context.log.info(f"Streaming {blob_name} to Postgres table {schema}.{table_name} in batches of {batch_size}...")

    # 2. Truncate the staging table once before starting
    with cloud_sql_engine.connect() as conn:
        context.log.info(f"Truncating {schema}.{table_name} table.")
        conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {schema}.{table_name};"))
        conn.commit()

    # 3. Stream from GCS and process in batches
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    batch_of_dicts = []
    total_rows_processed = 0

    with blob.open("r", encoding="utf-8") as f:
        for line in f:
            # Use the much faster standard json library to parse each line
            batch_of_dicts.append(json.loads(line))

            if len(batch_of_dicts) >= batch_size:
                # Once the batch is full, process and write it to the database
                df_batch = pd.DataFrame(batch_of_dicts)
                
                # Perform transformations on the small batch DataFrame
                df_batch.rename(columns={
                    "eco_name": "project_title",
                    "branch": "sub_ecosystems",
                    "repo_url": "repo",
                    "tags": "tags"
                }, inplace=True)
                df_batch['data_timestamp'] = data_timestamp

                # Append batch to postgres
                df_batch.to_sql(table_name, cloud_sql_engine, if_exists='append', index=False, schema=schema)
                
                total_rows_processed += len(batch_of_dicts)
                context.log.info(f"Processed and wrote a batch of {len(batch_of_dicts)} rows. Total rows: {total_rows_processed}")
                
                # Clear the list to free memory
                batch_of_dicts = []

        # 4. Process any remaining records in the last partial batch
        if batch_of_dicts:
            df_batch = pd.DataFrame(batch_of_dicts)
            df_batch.rename(columns={
                "eco_name": "project_title",
                "branch": "sub_ecosystems",
                "repo_url": "repo",
                "tags": "tags"
            }, inplace=True)
            df_batch['data_timestamp'] = data_timestamp

            df_batch.to_sql(table_name, cloud_sql_engine, if_exists='append', index=False, schema=schema)
            total_rows_processed += len(batch_of_dicts)
            context.log.info(f"Processed and wrote the final batch of {len(batch_of_dicts)} rows. Total rows: {total_rows_processed}")

    context.log.info("Successfully streamed all data from GCS to PostgreSQL.")
    return total_rows_processed


# (delete_old_gcs_files function remains the same)
def delete_old_gcs_files(context, bucket_name: str, days_old: int):
    """
    Scans a GCS bucket and deletes any blobs older than 4 weeks.
    """
    context.log.info(f"Starting cleanup of old files in GCS bucket: {bucket_name}...")
    gcs_resource = context.resources.gcs
    storage_client = gcs_resource.get_client()
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
    
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs()
        
        deleted_count = 0
        for blob in blobs:
            if blob.time_created < cutoff_date:
                context.log.info(f"Deleting old file: {blob.name} (created on {blob.time_created.strftime('%Y-%m-%d')})")
                blob.delete()
                deleted_count += 1
        
        if deleted_count > 0:
            context.log.info(f"Successfully deleted {deleted_count} old file(s) from {bucket_name}.")
        else:
            context.log.info(f"No files older than {days_old} days found in {bucket_name}.")
            
    except Exception as e:
        context.log.error(f"Could not complete cleanup of bucket {bucket_name}. Error: {e}")


# The main asset becomes much cleaner.
def create_crypto_ecosystems_project_json_asset(env_prefix: str):

    upstream_asset_key = AssetKey([env_prefix, "update_crypto_ecosystems_repo_and_run_export"])

    @dg.asset(
        key_prefix=env_prefix,
        name="crypto_ecosystems_project_json",
        ins={"update_crypto_ecosystems_repo_and_run_export": AssetIn(key=upstream_asset_key)},
        required_resource_keys={"gcs", "cloud_sql_postgres_resource", "active_env_config"},
        group_name="ingestion",
        description="This asset streams the project/repo list from GCS and loads it to a staging table in batches.",
    )
    def _crypto_ecosystems_project_json_env_specific(context, update_crypto_ecosystems_repo_and_run_export: str) -> dg.MaterializeResult:
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        raw_schema = env_config["raw_schema"]
        table_name = "crypto_ecosystems_raw_file_staging"
        gcs_file_path = update_crypto_ecosystems_repo_and_run_export
        data_timestamp = pd.Timestamp.now()
        
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        if not gcs_file_path:
            raise ValueError("GCS file path from upstream asset is empty or None.")

        try:
            # The new helper function handles everything
            total_rows = stream_gcs_to_postgres_in_batches(
                context,
                gcs_path=gcs_file_path,
                table_name=table_name,
                schema=raw_schema,
                batch_size=20000,
                data_timestamp=data_timestamp
            )
        except Exception as e:
            raise ValueError(f"Error streaming GCS file to database: {e}") from e

        # Get metadata for the MaterializeResult
        with cloud_sql_engine.connect() as conn:
            preview_df = pd.read_sql(text(f"SELECT * FROM {raw_schema}.{table_name} LIMIT 10"), conn)
            unique_project_count = conn.execute(text(f"SELECT COUNT(DISTINCT project_title) FROM {raw_schema}.{table_name}")).scalar_one()
            unique_repo_count = conn.execute(text(f"SELECT COUNT(DISTINCT repo) FROM {raw_schema}.{table_name}")).scalar_one()

        # Cleanup old GCS files
        delete_old_gcs_files(context, "crypto-ecosystems-export", 28)

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(total_rows),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "unique_project_count": dg.MetadataValue.int(unique_project_count),
                "unique_repo_count": dg.MetadataValue.int(unique_repo_count),
            }
        )

    return _crypto_ecosystems_project_json_env_specific


# define the asset that gets the active and archived status for the distinct repo list
# to accomodate multiple environments, we will use a factory function
def create_latest_active_distinct_github_project_repos_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="latest_active_distinct_github_project_repos",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="clean_data",
        tags={"github_api": "True"},
        automation_condition=dg.AutomationCondition.eager(),
    )
    def _latest_active_distinct_github_project_repos_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        raw_schema = env_config["raw_schema"]
        clean_schema = env_config["clean_schema"]

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** updated: this process is running in {env_config['env']} environment. *****************---------")

        def get_non_github_repo_status(repo_url, repo_source):
            """
            Checks if a non-GitHub repo exists and if it is archived.
            Returns a dictionary with 'is_active' and 'is_archived' status.
            """
            if repo_source == "bitbucket":
                try:
                    parts = repo_url.rstrip('/').split('/')
                    owner = parts[-2]
                    repo_slug = parts[-1].split('.')[0] if '.' in parts[-1] else parts[-1]
                except IndexError:
                    context.log.warning(f"Invalid Bitbucket URL format: {repo_url}")
                    return {'is_active': False, 'is_archived': None}

                api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}"
                response = requests.get(api_url)

                if response.status_code == 200:
                    # Bitbucket API doesn't have an 'archived' field, so we default to False.
                    return {'is_active': True, 'is_archived': False}
                else:
                    return {'is_active': False, 'is_archived': None}

            elif repo_source == "gitlab":
                try:
                    parts = repo_url.rstrip('/').split('/')
                    project_path = "/".join(parts[3:])
                    project_path_encoded = requests.utils.quote(project_path, safe='')
                except IndexError:
                    context.log.warning(f"Invalid GitLab URL format: {repo_url}")
                    return {'is_active': False, 'is_archived': None}

                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"
                try:
                    response = requests.get(api_url)
                    context.log.debug(f"Status Code: {response.status_code}, URL: {api_url}")

                    if response.status_code == 200:
                        data = response.json()
                        return {'is_active': True, 'is_archived': data.get('archived', False)}
                    else:
                        return {'is_active': False, 'is_archived': None}
                except Exception as e:
                    context.log.warning(f"Error checking GitLab repo: {e}")
                    return {'is_active': False, 'is_archived': None}
            else:
                return {'is_active': False, 'is_archived': None}

        def get_github_repo_status(repo_urls, gh_pat, repo_source):
            """
            Checks if a GitHub repository is active (not private) and if it's archived
            using the GraphQL API.
            """
            if not repo_urls:
                return {}, {}

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat}"}
            results = {}
            batch_size = 150 # Reduced batch size slightly for larger payload
            count_403_errors = 0
            count_502_errors = 0

            for i in range(0, len(repo_urls), batch_size):
                context.log.debug(f"processing batch: {i} - {i + batch_size}")
                # every 1000 batches, print the progress
                if i % 1000 == 0:
                    context.log.info(f"processing batch: {i} - {i + batch_size}")
                start_time = time.time()
                batch = repo_urls[i:i + batch_size]
                processed_in_batch = set()
                query = "query ("
                variables = {}

                for j, repo_url in enumerate(batch):
                    try:
                        parts = repo_url.rstrip('/').split('/')
                        owner = parts[-2]
                        name = parts[-1]
                        query += f"$owner{j}: String!, $name{j}: String!,"
                        variables[f"owner{j}"] = owner
                        variables[f"name{j}"] = name
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format: {repo_url}")
                        continue

                query = query.rstrip(",") + ") {\n"

                # UPDATED QUERY: Fetch both isPrivate and isArchived
                for j, repo_url in enumerate(batch):
                    query += f"""  repo{j}: repository(owner: $owner{j}, name: $name{j}) {{
                        isPrivate
                        isArchived
                    }}\n"""
                query += "}"

                max_retries = 8
                for attempt in range(max_retries):
                    try:
                        response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                        time.sleep(2.5)
                        response.raise_for_status()
                        data = response.json()

                        if 'errors' in data:
                            # Simplified error handling for brevity
                            context.log.warning(f"GraphQL Error in batch {i}: {data['errors'][0]['message']}")

                        if 'data' in data:
                            for j, repo_url in enumerate(batch):
                                if repo_url in processed_in_batch:
                                    continue

                                repo_data = data['data'].get(f'repo{j}')
                                if repo_data:
                                    # Store both active and archived status
                                    results[repo_url] = {
                                        "is_active": not repo_data.get('isPrivate', True),
                                        "is_archived": repo_data.get('isArchived', False),
                                        "repo_source": repo_source
                                    }
                                    processed_in_batch.add(repo_url)
                        break

                    except requests.exceptions.RequestException as e:
                        # Simplified retry logic for brevity
                        context.log.warning(f"Request exception on attempt {attempt+1}: {e}")
                        if isinstance(e, requests.exceptions.HTTPError):
                             if e.response.status_code in (502, 504): count_502_errors += 1
                             if e.response.status_code in (403, 429): count_403_errors += 1
                        if attempt == max_retries - 1:
                            context.log.warning("Max retries reached. Giving up on batch.")
                            break
                        time.sleep((2 ** attempt) + random.uniform(0, 1))

                # Handle any repos that failed all retries
                for repo_url in batch:
                    if repo_url not in processed_in_batch:
                        results[repo_url] = {
                            "is_active": False,
                            "is_archived": None,
                            "repo_source": repo_source
                        }

            return results, {"count_403_errors": count_403_errors, "count_502_errors": count_502_errors}

        # --- Main Execution Logic ---
        with cloud_sql_engine.connect() as conn:
            result = conn.execute(
                text(f"""select repo, repo_source from {clean_schema}.latest_distinct_project_repos""")
            )
            distinct_repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Process GitHub Repos
        context.log.info("Processing GitHub repos...")
        github_urls = distinct_repo_df[distinct_repo_df['repo_source'] == 'github']['repo'].tolist()
        # Access config directly from the typed config object
        gh_pat = github_api.get_client(config.key_name)
        github_results_dict, count_http_errors = get_github_repo_status(github_urls, gh_pat, 'github')

        # Convert dictionary to DataFrame, now including is_archived
        github_results_df = pd.DataFrame.from_dict(github_results_dict, orient='index').reset_index().rename(columns={'index': 'repo'})

        # Process non-GitHub Repos
        non_github_df = distinct_repo_df[distinct_repo_df['repo_source'] != 'github'].copy()
        if not non_github_df.empty:
            context.log.info("Found non-github repos. Getting active and archived status...")
            # Apply the function and expand the resulting dictionary into new columns
            status_df = non_github_df.apply(
                lambda row: get_non_github_repo_status(row['repo'], row['repo_source']),
                axis=1,
                result_type='expand'
            )
            # Join the new status columns back to the original dataframe
            non_github_results_df = non_github_df.join(status_df)
            # Combine GitHub and non-GitHub results
            results_df = pd.concat([github_results_df, non_github_results_df], ignore_index=True)
        else:
            results_df = github_results_df

        # Add timestamp and write to the database
        results_df['data_timestamp'] = pd.Timestamp.now()
        # Ensure correct boolean types for the database
        results_df['is_active'] = results_df['is_active'].astype('boolean')
        results_df['is_archived'] = results_df['is_archived'].astype('boolean')
        
        # Reorder columns for clarity
        final_cols = ['repo', 'repo_source', 'is_active', 'is_archived', 'data_timestamp']
        results_df = results_df[final_cols]
        
        results_df.to_sql('latest_active_distinct_project_repos', cloud_sql_engine, if_exists='replace', index=False, schema=raw_schema)

        # --- Metadata for Dagster UI ---
        with cloud_sql_engine.connect() as conn:
            preview_query = text(f"select count(*) from {raw_schema}.latest_active_distinct_project_repos")
            row_count = conn.execute(preview_query).scalar_one()

            preview_query = text(f"select * from {raw_schema}.latest_active_distinct_project_repos limit 10")
            result_df = pd.DataFrame(conn.execute(preview_query).fetchall(), columns=conn.execute(preview_query).keys())

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                "count_http_403_errors": dg.MetadataValue.int(count_http_errors['count_403_errors']),
                "count_http_502_errors": dg.MetadataValue.int(count_http_errors['count_502_errors']),
            }
        )

    return _latest_active_distinct_github_project_repos_env_specific


# define the asset that gets the stargaze count for a repo
# to accomodate multiple environments, we will use a factory function
def create_github_project_repos_stargaze_count_asset(env_prefix: str):
    """
    Factory function to create the github_project_repos_stargaze_count asset
    with an environment-specific key_prefix.
    """
    @dg.asset(
        key_prefix=env_prefix,  # <<< This is the key change for namespacing
        name="github_project_repos_stargaze_count", # This is the base name of the asset
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion", # Group name
        tags={"github_api": "True"},
    )
    def _github_project_repos_stargaze_count_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        # get the github personal access token
        gh_pat = github_api.get_client(config.key_name)

        def get_non_github_repo_stargaze_count(repo_url, repo_source):

            context.log.info(f"processing non-githubrepo: {repo_url}")

            # add a 1 second delay to avoid rate limiting
            # note: this is simplified solution but there are not many non-github repos
            time.sleep(0.5)

            if repo_source == "bitbucket":
                # Extract owner and repo_slug from the URL
                try:
                    parts = repo_url.rstrip('/').split('/')
                    owner = parts[-2]
                    repo_slug = parts[-1]
                    if '.' in repo_slug:
                        repo_slug = repo_slug.split('.')[0]
                except IndexError:
                    context.log.warning(f"Invalid Bitbucket URL format: {repo_url}")
                    return None

                try:
                    # Construct the correct Bitbucket API endpoint
                    api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}"

                    response = requests.get(api_url)

                    # check if the response is successful
                    response.raise_for_status()

                    watchers_url = response['links']['watchers']['href']
                    watchers_response = requests.get(watchers_url)
                    watchers_response.raise_for_status()
                    watchers_data = watchers_response.json()

                    # Get the watcher count from the 'size' field
                    return watchers_data['size']

                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from Bitbucket API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}")
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None

            elif repo_source == "gitlab":
                try:
                    parts = repo_url.rstrip('/').split('/')
                    project_path = "/".join(parts[3:])
                    project_path_encoded = requests.utils.quote(project_path, safe='')
                except IndexError:
                    context.log.warning(f"Invalid GitLab URL format: {repo_url}")
                    return None

                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

                try:
                    response = requests.get(api_url)  # No headers needed for unauthenticated access
                    response.raise_for_status()

                    # return the stargaze count
                    return response.json()['star_count']
                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from GitLab API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}") 
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None
            else:
                return None

        def get_github_repo_stargaze_count(repo_urls, gh_pat):
            """
            Queries the stargaze count for a GitHub repository using the GraphQL API.

            Args:
                repo_urls: A list of GitHub repository URLs.

            Returns:
                A dictionary mapping each repository URL to the stargaze count.
            """

            if not repo_urls:  # Handle empty input list
                return [], 0

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat}"}
            results = {}  # Store results: {url: stargaze_count}
            batch_size = 180  # Adjust as needed
            cpu_time_used = 0
            real_time_used = 0
            real_time_window = 60
            cpu_time_limit = 50
            count_403_errors = 0
            count_502_errors = 0
            batch_time_history = []

            for i in range(0, len(repo_urls), batch_size):
                context.log.info(f"processing batch: {i} - {i + batch_size}")
                # calculate the time it takes to process the batch
                start_time = time.time()
                batch = repo_urls[i:i + batch_size]
                processed_in_batch = set()  # Track successfully processed repos *within this batch*
                query = "query ("  # Start the query definition
                variables = {}

                # 1. Declare variables in the query definition
                for j, repo_url in enumerate(batch):
                    try:
                        parts = repo_url.rstrip('/').split('/')
                        owner = parts[-2]
                        name = parts[-1]
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format: {repo_url}")
                        # don't return here, return errors at end of batch
                        continue

                    query += f"$owner{j}: String!, $name{j}: String!,"  # Declare variables
                    variables[f"owner{j}"] = owner
                    variables[f"name{j}"] = name

                query = query.rstrip(",")  # Remove trailing comma
                query += ") {\n"  # Close the variable declaration

                # 2. Construct the query body (using the declared variables)
                for j, repo_url in enumerate(batch):
                    query += f"""  repo{j}: repository(owner: $owner{j}, name: $name{j}) {{
                        stargazers {{
                            totalCount
                        }}
                    }}\n"""

                query += "}"

                base_delay = 1
                max_delay = 60
                max_retries = 8

                for attempt in range(max_retries):
                    context.log.info(f"attempt: {attempt}")
                    
                    try:
                        if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                            extra_delay = (cpu_time_used - cpu_time_limit) / 2
                            extra_delay = max(1, extra_delay)
                            context.log.info(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                            time.sleep(extra_delay)
                            context.log.info(f"resetting cpu_time_used and real_time_used to 0")
                            cpu_time_used = 0
                            real_time_used = 0
                            start_time = time.time()
                        elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info(f"real time limit reached without CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                            context.log.info(f"real time limit reached. CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info('cpu time limit not reached. Continuing...')

                        response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)

                        time_since_start = time.time() - start_time
                        context.log.info(f"time_since_start: {time_since_start:.2f} seconds")
                        time.sleep(3)  # Consistent delay
                        
                        # use raise for status to catch errors
                        response.raise_for_status()
                        data = response.json()

                        if 'errors' in data:
                            context.log.info(f"Status Code: {response.status_code}")
                            # Extract rate limit information from headers
                            context.log.info(" \n resource usage tracking:")
                            rate_limit_info = {
                                'remaining': response.headers.get('x-ratelimit-remaining'),
                                'used': response.headers.get('x-ratelimit-used'),
                                'reset': response.headers.get('x-ratelimit-reset'),
                                'retry_after': response.headers.get('retry-after')
                            }
                            context.log.info(f"Rate Limit Info: {rate_limit_info}\n")

                            for error in data['errors']:
                                if error['type'] == 'RATE_LIMITED':
                                    reset_at = response.headers.get('X-RateLimit-Reset')
                                    if reset_at:
                                        delay = int(reset_at) - int(time.time()) + 1
                                        delay = max(1, delay)
                                        delay = min(delay, max_delay)
                                        context.log.info(f"Rate limited.  Waiting for {delay} seconds...")
                                        time.sleep(delay)
                                        continue  # Retry the entire batch
                                else:
                                    context.log.info(f"GraphQL Error: {error}") #Print all the errors.

                        # write the url and stargaze count to the database
                        if 'data' in data:
                            for j, repo_url in enumerate(batch):
                                if repo_url in processed_in_batch:  # CRUCIAL CHECK
                                    continue  # Skip if already processed

                                repo_data = data['data'].get(f'repo{j}')
                                if repo_data:
                                    results[repo_url] = repo_data['stargazers']['totalCount']
                                    processed_in_batch.add(repo_url)  # Mark as processed
                                else:
                                    context.log.info(f"repo_data is empty for repo: {repo_url}\n")
                                    # don't return here, return errors at end of batch
                        break

                    except requests.exceptions.RequestException as e:
                        context.log.warning(f"there was a request exception on attempt: {attempt}\n")
                        context.log.warning(f"procesing batch: {batch}\n")
                        context.log.warning(f"Status Code: {response.status_code}")

                        # Extract rate limit information from headers
                        context.log.warning(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        context.log.warning(f"Rate Limit Info: {rate_limit_info}\n")

                        context.log.warning(f"the error is: {e}\n")
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached or unrecoverable error for batch. Giving up.")
                            # don't return here, return errors at end of batch
                            break

                        # rate limit handling
                        if isinstance(e, requests.exceptions.HTTPError):
                            if e.response.status_code in (502, 504):
                                count_502_errors += 1
                                context.log.warning(f"This process has generated {count_502_errors} 502/504 errors in total.")
                                delay = 1
                                context.log.warning(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                            elif e.response.status_code in (403, 429):
                                count_403_errors += 1
                                context.log.warning(f"This process has generated {count_403_errors} 403/429 errors in total.")
                                retry_after = e.response.headers.get('Retry-After')
                                if retry_after:
                                    delay = int(retry_after)
                                    context.log.warning(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue
                                else:
                                    delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                    context.log.warning(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                    time.sleep(delay)
                                    continue
                        else:
                            delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                            context.log.warning(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)

                    except KeyError as e:
                        context.log.warning(f"KeyError: {e}. Response: {data}")
                        # Don't append here; handle errors at the end
                        break
                    except Exception as e:
                        context.log.warning(f"An unexpected error occurred: {e}")
                        # Don't append here; handle errors at the end
                        break

                # Handle any repos that failed *all* retries (or were invalid URLs)
                for repo_url in batch:
                    if repo_url not in processed_in_batch:
                        results[repo_url] = None
                        context.log.warning(f"adding repo to results after max retries, or was invalid url: {repo_url}")

                # calculate the time it takes to process the batch
                end_time = time.time()
                batch_time = end_time - start_time
                cpu_time_used += time_since_start
                real_time_used += batch_time
                batch_time_history.append(batch_time)
                if batch_time_history and len(batch_time_history) > 10:
                    context.log.info(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
                context.log.info(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
                context.log.info(f"time taken to process batch {i}: {batch_time:.2f} seconds")
                context.log.info(f"Total CPU time used: {cpu_time_used:.2f} seconds")
                context.log.info(f"Total real time used: {real_time_used:.2f} seconds")

            return results, {
                'count_403_errors': count_403_errors,
                'count_502_errors': count_502_errors
            }

        # Execute the query
        with cloud_sql_engine.connect() as conn:

            # query the latest_distinct_project_repos table to get the distinct repo list
            result = conn.execute(
                text(f"""
                select 
                    repo, 
                    repo_source 
                from {clean_schema}.latest_active_distinct_project_repos 
                where is_active = true 
                """)
                    )
            repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Filter for GitHub URLs
        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

        # get github pat
        gh_pat = github_api.get_client(config.key_name)

        results = get_github_repo_stargaze_count(github_urls, gh_pat)

        github_results = results[0]
        count_http_errors_github_api = results[1]

        # write results to pandas dataframe
        results_df = pd.DataFrame(github_results.items(), columns=['repo', 'stargaze_count'])

        # now get non-github repos urls
        non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

        # if non_github_urls is not empty, get stargaze count
        if not non_github_results_df.empty:
            context.log.info("found non-github repos. Getting repo stargaze count...")
            # apply distinct_repo_df['repo'] to get stargaze count
            non_github_results_df['stargaze_count'] = non_github_results_df.apply(
                lambda row: get_non_github_repo_stargaze_count(row['repo'], row['repo_source']), axis=1
            )

            # drop the repo_source column
            non_github_results_df = non_github_results_df.drop(columns=['repo_source'])

            # append non_github_urls to results_df
            results_df = pd.concat([results_df, non_github_results_df])

        # add unix datetime column
        results_df['data_timestamp'] = pd.Timestamp.now()

        # write results to database
        results_df.to_sql('project_repos_stargaze_count', cloud_sql_engine, if_exists='append', index=False, schema=raw_schema)

        with cloud_sql_engine.connect() as conn:
            # capture asset metadata
            preview_query = text(f"select count(*) from {raw_schema}.project_repos_stargaze_count")
            result = conn.execute(preview_query)
            # Fetch all rows into a list of tuples
            row_count = result.fetchone()[0]

            preview_query = text(f"select * from {raw_schema}.project_repos_stargaze_count limit 10")
            result = conn.execute(preview_query)
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                "count_403_errors": dg.MetadataValue.int(count_http_errors_github_api['count_403_errors']),
                "count_502_errors": dg.MetadataValue.int(count_http_errors_github_api['count_502_errors'])
            }
        )

    return _github_project_repos_stargaze_count_env_specific # Return the decorated function


# define the asset that gets the fork count for a repo
# to accomodate multiple environments, we will use a factory function
def create_github_project_repos_fork_count_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="github_project_repos_fork_count",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
    )
    def _github_project_repos_fork_count_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        # get the github personal access token
        gh_pat = github_api.get_client(config.key_name)

        def get_non_github_repo_fork_count(repo_url, repo_source):

            context.log.info(f"processing non-githubrepo: {repo_url}")

            # add a 1 second delay to avoid rate limiting
            # note: this is simplified solution but there are not many non-github repos
            time.sleep(0.5)

            if repo_source == "bitbucket":
                # Extract owner and repo_slug from the URL
                try:
                    parts = repo_url.rstrip('/').split('/')
                    owner = parts[-2]
                    repo_slug = parts[-1]
                    if '.' in repo_slug:
                        repo_slug = repo_slug.split('.')[0]
                except IndexError:
                    context.log.warning(f"Invalid Bitbucket URL format: {repo_url}")
                    return None

                try:
                    # Construct the correct Bitbucket API endpoint
                    api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}"

                    response = requests.get(api_url)

                    # check if the response is successful
                    response.raise_for_status()

                    forks_url = response['links']['forks']['href']
                    forks_response = requests.get(forks_url)
                    forks_response.raise_for_status()
                    forks_data = forks_response.json()

                    # Get the watcher count from the 'size' field
                    return forks_data['size']

                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from Bitbucket API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}")
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None

            elif repo_source == "gitlab":
                try:
                    parts = repo_url.rstrip('/').split('/')
                    project_path = "/".join(parts[3:])
                    project_path_encoded = requests.utils.quote(project_path, safe='')
                except IndexError:
                    context.log.warning(f"Invalid GitLab URL format: {repo_url}")
                    return None

                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

                try:
                    response = requests.get(api_url)  # No headers needed for unauthenticated access
                    response.raise_for_status()

                    # return the fork count
                    return response.json()['forks_count']
                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from GitLab API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}") 
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None
            else:
                return None

        def get_github_repo_fork_count(repo_urls, gh_pat):
            """
            Queries the fork count for a GitHub repository using the GraphQL API.

            Args:
                repo_urls: A list of GitHub repository URLs.

            Returns:
                A dictionary mapping each repository URL to the fork count.
            """

            if not repo_urls:  # Handle empty input list
                return [], 0

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat}"}
            results = {}  # Store results: {url: fork_count}
            batch_size = 180  # Adjust as needed
            cpu_time_used = 0
            real_time_used = 0
            real_time_window = 60
            cpu_time_limit = 50
            count_502_errors = 0
            count_403_errors = 0
            batch_time_history = []

            for i in range(0, len(repo_urls), batch_size):
                context.log.info(f"processing batch: {i} - {i + batch_size}")
                # calculate the time it takes to process the batch
                start_time = time.time()
                batch = repo_urls[i:i + batch_size]
                processed_in_batch = set()  # Track successfully processed repos *within this batch*
                query = "query ("  # Start the query definition
                variables = {}

                # 1. Declare variables in the query definition
                for j, repo_url in enumerate(batch):
                    try:
                        parts = repo_url.rstrip('/').split('/')
                        owner = parts[-2]
                        name = parts[-1]
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format: {repo_url}")
                        continue

                    query += f"$owner{j}: String!, $name{j}: String!,"  # Declare variables
                    variables[f"owner{j}"] = owner
                    variables[f"name{j}"] = name

                query = query.rstrip(",")  # Remove trailing comma
                query += ") {\n"  # Close the variable declaration

                # 2. Construct the query body (using the declared variables)
                for j, repo_url in enumerate(batch):
                    query += f"""  repo{j}: repository(owner: $owner{j}, name: $name{j}) {{
                        forkCount
                    }}\n"""

                query += "}"

                base_delay = 1
                max_delay = 60
                max_retries = 8

                for attempt in range(max_retries):
                    context.log.info(f"attempt: {attempt}")
                    
                    try:
                        if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                            extra_delay = (cpu_time_used - cpu_time_limit) / 2
                            extra_delay = max(1, extra_delay)
                            context.log.info(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                            time.sleep(extra_delay)
                            context.log.info(f"resetting cpu_time_used and real_time_used to 0")
                            cpu_time_used = 0
                            real_time_used = 0
                            start_time = time.time()
                        elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info(f"real time limit reached without CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                            context.log.info(f"real time limit reached. CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info('cpu time limit not reached. Continuing...')

                        response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                        time_since_start = time.time() - start_time
                        context.log.info(f"time_since_start: {time_since_start:.2f} seconds")
                        time.sleep(3)  # Consistent delay

                        # use raise for status to catch errors
                        response.raise_for_status()
                        data = response.json()

                        if 'errors' in data:
                            context.log.info(f"Status Code: {response.status_code}")
                            # Extract rate limit information from headers
                            context.log.info(" \n resource usage tracking:")
                            rate_limit_info = {
                                'remaining': response.headers.get('x-ratelimit-remaining'),
                                'used': response.headers.get('x-ratelimit-used'),
                                'reset': response.headers.get('x-ratelimit-reset'),
                                'retry_after': response.headers.get('retry-after')
                            }
                            context.log.info(f"Rate Limit Info: {rate_limit_info}\n")

                            for error in data['errors']:
                                if error['type'] == 'RATE_LIMITED':
                                    reset_at = response.headers.get('X-RateLimit-Reset')
                                    if reset_at:
                                        delay = int(reset_at) - int(time.time()) + 1
                                        delay = max(1, delay)
                                        delay = min(delay, max_delay)
                                        context.log.warning(f"Rate limited.  Waiting for {delay} seconds...")
                                        time.sleep(delay)
                                        continue  # Retry the entire batch
                                else:
                                    context.log.warning(f"GraphQL Error: {error}") #Print all the errors.

                        # write the url and fork count to the database
                        if 'data' in data:
                            for j, repo_url in enumerate(batch):
                                if repo_url in processed_in_batch:  # CRUCIAL CHECK
                                    continue  # Skip if already processed
                                repo_data = data['data'].get(f'repo{j}')
                                if repo_data:
                                    results[repo_url] = repo_data['forkCount']
                                    processed_in_batch.add(repo_url)  # Mark as processed
                                else:
                                    context.log.warning(f"repo_data is empty for repo: {repo_url}\n")
                        break

                    except requests.exceptions.RequestException as e:
                        context.log.warning(f"there was a request exception on attempt: {attempt}\n")
                        context.log.warning(f"procesing batch: {batch}\n")
                        context.log.warning(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        context.log.warning(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        context.log.warning(f"Rate Limit Info: {rate_limit_info}\n")

                        context.log.warning(f"the error is: {e}\n")
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached or unrecoverable error for batch. Giving up.")
                            break
                        # --- Rate Limit Handling (REST API style - for 403/429) ---
                        if isinstance(e, requests.exceptions.HTTPError):
                            if e.response.status_code in (502, 504):
                                count_502_errors += 1
                                context.log.warning(f"This process has generated {count_502_errors} 502/504 errors in total.")
                                delay = 1
                                context.log.warning(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                            elif e.response.status_code in (403, 429):
                                count_403_errors += 1
                                context.log.warning(f"This process has generated {count_403_errors} 403/429 errors in total.")
                                retry_after = response.headers.get('Retry-After')
                                if retry_after:
                                    delay = int(retry_after)
                                    context.log.warning(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue
                                else:
                                    delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                    context.log.warning(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                    time.sleep(delay)
                                    continue
                        else:
                            delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                            context.log.warning(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)

                    except KeyError as e:
                        context.log.warning(f"KeyError: {e}. Response: {data}")
                        # Don't append here; handle errors at the end
                        break
                    except Exception as e:
                        context.log.warning(f"An unexpected error occurred: {e}")
                        # Don't append here; handle errors at the end
                        break

                # Handle any repos that failed *all* retries (or were invalid URLs)
                for repo_url in batch:
                    if repo_url not in processed_in_batch:
                        results[repo_url] = None
                        context.log.warning(f"adding repo to results after max retries, or was invalid url: {repo_url}")

                end_time = time.time()
                batch_time = end_time - start_time
                cpu_time_used += time_since_start
                real_time_used += batch_time
                batch_time_history.append(batch_time)
                if batch_time_history and len(batch_time_history) > 10:
                    context.log.info(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
                context.log.info(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
                context.log.info(f"time taken to process batch {i}: {batch_time:.2f} seconds")
                context.log.info(f"Total CPU time used: {cpu_time_used:.2f} seconds")
                context.log.info(f"Total real time used: {real_time_used:.2f} seconds")

            return results, {
                'count_403_errors': count_403_errors,
                'count_502_errors': count_502_errors
            }

        # Execute the query
        with cloud_sql_engine.connect() as conn:

            # query the latest_distinct_project_repos table to get the distinct repo list
            result = conn.execute(
                text(f"""select repo, repo_source from {clean_schema}.latest_active_distinct_project_repos where is_active = true""")
                    )
            repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Filter for GitHub URLs
        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

        context.log.info(f"number of github urls: {len(github_urls)}")

        results = get_github_repo_fork_count(github_urls, gh_pat)

        github_results = results[0]
        count_http_errors_github_api = results[1]

        # write results to pandas dataframe
        results_df = pd.DataFrame(github_results.items(), columns=['repo', 'fork_count'])

        # now get non-github repos urls
        non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

        # if non_github_urls is not empty, get fork count
        if not non_github_results_df.empty:
            context.log.info("found non-github repos. Getting repo fork count...")
            # apply distinct_repo_df['repo'] to get fork count
            non_github_results_df['fork_count'] = non_github_results_df.apply(
                lambda row: get_non_github_repo_fork_count(row['repo'], row['repo_source']), axis=1
            )

            # drop the repo_source column
            non_github_results_df = non_github_results_df.drop(columns=['repo_source'])

            # append non_github_urls to results_df
            results_df = pd.concat([results_df, non_github_results_df])

        # add unix datetime column
        results_df['data_timestamp'] = pd.Timestamp.now()

        # write results to database
        results_df.to_sql('project_repos_fork_count', cloud_sql_engine, if_exists='append', index=False, schema=raw_schema)

        with cloud_sql_engine.connect() as conn:
            # capture asset metadata
            preview_query = text(f"select count(*) from {raw_schema}.project_repos_fork_count")
            result = conn.execute(preview_query)
            # Fetch all rows into a list of tuples
            row_count = result.fetchone()[0]

            preview_query = text(f"select * from {raw_schema}.project_repos_fork_count limit 10")
            result = conn.execute(preview_query)
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                "count_403_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_403_errors']),
                "count_502_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_502_errors'])
            }
        )

    return _github_project_repos_fork_count_env_specific

# to accomodate multiple environments, we will use a factory function
def create_github_project_repos_languages_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="github_project_repos_languages",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
    )
    def _github_project_repos_languages_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        # get the github personal access token
        gh_pat = github_api.get_client(config.key_name)

        def get_non_github_repo_languages(repo_url, repo_source):

            context.log.info(f"processing non-githubrepo: {repo_url}")

            # add a 0.25 second delay to avoid rate limiting
            # note: this is simplified solution but there are not many non-github repos
            time.sleep(0.5)

            if repo_source == "bitbucket":
                # Extract owner and repo_slug from the URL
                try:
                    parts = repo_url.rstrip('/').split('/')
                    owner = parts[-2]
                    repo_slug = parts[-1]
                    if '.' in repo_slug:
                        repo_slug = repo_slug.split('.')[0]
                except IndexError:
                    context.log.warning(f"Invalid Bitbucket URL format: {repo_url}")
                    return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}

                try:
                    # Construct the correct Bitbucket API endpoint
                    api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}"

                    response = requests.get(api_url)

                    # check if the response is successful
                    response.raise_for_status()

                    primary_language = response.json()['language']

                    # Get the languages
                    return {'repo': repo_url, 'language_name': primary_language, 'size': None, 'repo_languages_total_bytes': None}

                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from Bitbucket API: {e}")
                    return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}")
                    return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}

            elif repo_source == "gitlab":
                try:
                    parts = repo_url.rstrip('/').split('/')
                    project_path = "/".join(parts[3:])
                    project_path_encoded = requests.utils.quote(project_path, safe='')
                except IndexError:
                    context.log.warning(f"Invalid GitLab URL format: {repo_url}")
                    return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}

                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

                try:
                    response = requests.get(api_url)  # No headers needed for unauthenticated access
                    response.raise_for_status()

                    # now access languages endpoint
                    languages_url = f"https://gitlab.com/api/v4/projects/{response.json()['id']}/languages"
                    languages_response = requests.get(languages_url)
                    languages_response.raise_for_status()

                    # get the language data
                    language_data = languages_response.json()
                    
                    # loop through each language in the response to build a list of language name and size
                    # keep only the language name with the highest size
                    language_list = []
                    for language, size in language_data.items():
                        language_list.append({'language_name': language, 'size': size})
                    
                    # return the language data
                    if language_list:
                        # order the language list by size
                        language_list = sorted(language_list, key=lambda x: x['size'], reverse=True)
                        # capture the top language
                        top_language = language_list[0]['language_name']
                        return {'repo': repo_url, 'language_name': top_language, 'size': None, 'repo_languages_total_bytes': None}
                    else:
                        return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
                        
                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from GitLab API: {e}")
                    return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}") 
                    return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
            else:
                return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}

        def get_github_repo_languages(repo_urls, gh_pat):
            """
            Queries the languages for a GitHub repository using the GraphQL API.

            Args:
                repo_urls: A list of GitHub repository URLs.

            Returns:
                A list of dictionaries, where each dictionary represents a repo and
                its language data.  Returns an empty list if no repos are provided.
            """
            if not repo_urls:  # Handle empty input list
                return [], 0

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat}"}
            results = []
            batch_size = 150  # Adjust as needed
            cpu_time_used = 0
            real_time_used = 0
            real_time_window = 60
            cpu_time_limit = 50
            count_403_errors = 0
            count_502_errors = 0
            batch_time_history = []

            for i in range(0, len(repo_urls), batch_size):
                context.log.info(f"processing batch: {i} - {i + batch_size}")
                start_time = time.time()
                batch = repo_urls[i:i + batch_size]
                processed_in_batch = set()  # Track successfully processed repos *within this batch*

                query = "query ("
                variables = {}

                for j, repo_url in enumerate(batch):
                    try:
                        parts = repo_url.rstrip('/').split('/')
                        owner = parts[-2]
                        name = parts[-1]
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format: {repo_url}")
                        # No need to append here, handle at the end of the batch loop
                        continue

                    query += f"$owner{j}: String!, $name{j}: String!,"
                    variables[f"owner{j}"] = owner
                    variables[f"name{j}"] = name

                query = query.rstrip(",")
                query += ") {\n"

                for j, repo_url in enumerate(batch):
                    query += f"""  repo{j}: repository(owner: $owner{j}, name: $name{j}) {{
                        url
                        languages(first: 100) {{
                            edges {{
                            node {{
                                name
                            }}
                            size
                            }}
                            totalSize
                        }}
                    }}\n"""

                query += "}"

                base_delay = 1
                max_delay = 60
                max_retries = 8

                for attempt in range(max_retries):
                    context.log.info(f"attempt: {attempt}")

                    try:
                        if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                            extra_delay = (cpu_time_used - cpu_time_limit) / 2
                            extra_delay = max(1, extra_delay)
                            context.log.info(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                            time.sleep(extra_delay)
                            context.log.info(f"resetting cpu_time_used and real_time_used to 0")
                            cpu_time_used = 0
                            real_time_used = 0
                            start_time = time.time()
                        elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info(f"real time limit reached without CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                            context.log.info(f"real time limit reached. CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info('cpu time limit not reached. Continuing...')

                        response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                        time_since_start = time.time() - start_time
                        context.log.info(f"time_since_start: {time_since_start:.2f} seconds")
                        time.sleep(2.5)  # Consistent delay

                        response.raise_for_status()
                        data = response.json()

                        if 'errors' in data:
                            context.log.info(f"Status Code: {response.status_code}")
                            context.log.info(" \nresource usage tracking:")
                            rate_limit_info = {
                                'remaining': response.headers.get('x-ratelimit-remaining'),
                                'used': response.headers.get('x-ratelimit-used'),
                                'reset': response.headers.get('x-ratelimit-reset'),
                                'retry_after': response.headers.get('retry-after')
                            }
                            context.log.info(f"Rate Limit Info: {rate_limit_info}\n")

                            for error in data['errors']:
                                if error['type'] == 'RATE_LIMITED':
                                    reset_at = response.headers.get('X-RateLimit-Reset')
                                    if reset_at:
                                        delay = int(reset_at) - int(time.time()) + 1
                                        delay = max(1, delay)
                                        delay = min(delay, max_delay)
                                        context.log.warning(f"Rate limited.  Waiting for {delay} seconds...")
                                        time.sleep(delay)
                                        continue  # Retry the entire batch

                                    else:
                                        context.log.warning(f"GraphQL Error: {error}")  # Print all the errors.

                        if 'data' in data:
                            for j, repo_url in enumerate(batch):
                                if repo_url in processed_in_batch:  # CRUCIAL CHECK
                                    continue  # Skip if already processed

                                repo_data = data['data'].get(f'repo{j}')
                                if repo_data:
                                    languages_data = []
                                    for edge in repo_data['languages']['edges']:
                                        languages_data.append({
                                            'language_name': edge['node']['name'],
                                            'size': edge['size']
                                        })
                                    results.append({
                                        'repo': repo_url,
                                        'languages_data': languages_data,
                                        'total_size': repo_data['languages']['totalSize']
                                    })
                                    processed_in_batch.add(repo_url)  # Mark as processed

                                else:
                                    context.log.warning(f"repo_data is empty for repo: {repo_url}\n")
                                    # Don't append here; handle missing data at the end
                        break  # Exit retry loop if successful

                    except requests.exceptions.RequestException as e:
                    # ... (Your existing exception handling - remains largely unchanged) ...
                        context.log.warning(f"there was a request exception on attempt: {attempt}\n")
                        context.log.warning(f"procesing batch: {batch}\n")
                        context.log.warning(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        context.log.warning(" \nresource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        context.log.warning(f"Rate Limit Info: {rate_limit_info}\n")

                        context.log.warning(f"the error is: {e}\n")
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached or unrecoverable error for batch. Giving up.")
                            # Don't append here; handle failures at the end
                            break

                        if isinstance(e, requests.exceptions.HTTPError):
                            if e.response.status_code in (502, 504):
                                count_502_errors += 1
                                context.log.warning(f"This process has generated {count_502_errors} 502/504 errors in total.")
                                delay = 1
                                context.log.warning(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                            elif e.response.status_code in (403, 429):
                                count_403_errors += 1
                                context.log.warning(f"This process has generated {count_403_errors} 403/429 errors in total.")
                                retry_after = response.headers.get('Retry-After')
                                if retry_after:
                                    delay = int(retry_after)
                                    context.log.warning(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue
                                else:
                                    delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                    context.log.warning(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                    time.sleep(delay)
                                    continue
                        else:
                            delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                            context.log.warning(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)

                    except KeyError as e:
                        context.log.warning(f"KeyError: {e}. Response: {data}")
                        # Don't append here; handle errors at the end
                        break
                    except Exception as e:
                        context.log.warning(f"An unexpected error occurred: {e}")
                        # Don't append here; handle errors at the end
                        break

                # Handle any repos that failed *all* retries (or were invalid URLs)
                for repo_url in batch:
                    if repo_url not in processed_in_batch:
                        results.append({'repo': repo_url, 'languages_data': None, 'total_size': None})
                        context.log.warning(f"adding repo to results after max retries, or was invalid url: {repo_url}")

                end_time = time.time()
                batch_time = end_time - start_time
                cpu_time_used += time_since_start
                real_time_used += batch_time
                batch_time_history.append(batch_time)
                if batch_time_history and len(batch_time_history) > 10:
                    context.log.info(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
                context.log.info(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
                context.log.info(f"time taken to process batch {i}: {batch_time:.2f} seconds")
                context.log.info(f"Total CPU time used: {cpu_time_used:.2f} seconds")
                context.log.info(f"Total real time used: {real_time_used:.2f} seconds")

            return results, {"count_403_errors": count_403_errors, "count_502_errors": count_502_errors}

        # Execute the query
        with cloud_sql_engine.connect() as conn:

            # query the latest_distinct_project_repos table to get the distinct repo list
            result = conn.execute(
                text(f"select repo, repo_source from {clean_schema}.latest_active_distinct_project_repos where is_active = true")
            )
            repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Filter for GitHub URLs
        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

        context.log.info(f"number of github urls: {len(github_urls)}")

        results = get_github_repo_languages(github_urls, gh_pat)

        github_results = results[0]
        count_http_errors_github_api = results[1]

        # write results to pandas dataframe
        results_df = pd.DataFrame(github_results)

        # Function to unpack dictionary and create new rows
        def unpack_list(row, column):
            repo_url = row['repo']
            repo_languages_total_bytes = row['total_size']
            languages_list = row[column]
            data = []
            if repo_languages_total_bytes == 0:
                data.append((repo_url, None, None, repo_languages_total_bytes))
            else:
                if languages_list:
                    for language in languages_list:
                        # check if language is not empty
                        if language is not None:
                            data.append((repo_url, language['language_name'], language['size'], repo_languages_total_bytes))  # Create a tuple for each value
                        else:
                            data.append((repo_url, None, None, repo_languages_total_bytes))
                else:
                    context.log.warning(f"languages_list is empty for repo: {repo_url}")
                    data.append((repo_url, None, None, repo_languages_total_bytes))
            return data

        # ########################## github_repo_languages
        # check if results_df is not empty
        if not results_df.empty:
            new_rows = results_df[['repo', 'languages_data', 'total_size']].apply(lambda row: unpack_list(row, 'languages_data'), axis=1).explode()

            # Create a new DataFrame from the unpacked data
            unpacked_df = pd.DataFrame(new_rows.tolist(), columns=['repo', 'language_name', 'size', 'repo_languages_total_bytes'])

            # print column names
            context.log.info("unpacked_df column names:")
            context.log.info(unpacked_df.columns)

        # now get non-github repos urls
        non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

        # if non_github_urls is not empty, get language data
        if not non_github_results_df.empty:
            context.log.info("found non-github repos. Getting repo language data...")
            # Use list comprehension to get data for each non-github repo
            language_data = [
                get_non_github_repo_languages(row['repo'], row['repo_source'])
                for _, row in non_github_results_df.iterrows()  # Or use itertuples for slight improvement
            ]

            # create a df
            non_github_results_df = pd.DataFrame(language_data)

            # print column names
            context.log.info("non_github_results_df column names:")
            context.log.info(non_github_results_df.columns)

        # append non_github_urls to unpacked_df
        if not unpacked_df.empty and not non_github_results_df.empty:
            # Both DataFrames are not empty, so concatenate them
            all_repos_df = pd.concat([unpacked_df, non_github_results_df], ignore_index=True)
        elif not unpacked_df.empty:
            # unpacked_df is not empty, use it as results_df
            all_repos_df = unpacked_df
        elif not non_github_results_df.empty:
            # non_github_results_df is not empty, use it as results_df
            all_repos_df = non_github_results_df
        else:
            # Both DataFrames are empty
            all_repos_df = pd.DataFrame()  # Create an empty DataFrame

        # pandas deduplication
        if not all_repos_df.empty:  # Only deduplicate if there's data
            all_repos_df.drop_duplicates(subset=['repo', 'language_name', 'repo_languages_total_bytes'], keep='first', inplace=True)
            # Add data_timestamp 
            all_repos_df['data_timestamp'] = pd.Timestamp.now()

        # write results to database
        # wrap in try except
        try:
            if not all_repos_df.empty:
                all_repos_df.to_sql(
                    'project_repos_languages', 
                    cloud_sql_engine, 
                    if_exists='append', 
                    index=False, 
                    schema=raw_schema,
                    dtype={
                        "size": sqlalchemy.types.BIGINT,
                        "repo_languages_total_bytes": sqlalchemy.types.BIGINT
                        }
                    )

                # create variable to store the count of rows written to the database
                row_count_this_run = all_repos_df.shape[0]

                with cloud_sql_engine.connect() as conn:
                    # capture asset metadata
                    preview_query = text(f"select count(*) from {raw_schema}.project_repos_languages")
                    result = conn.execute(preview_query)
                    # Fetch all rows into a list of tuples
                    row_count = result.fetchone()[0]

                    preview_query = text(f"select * from {raw_schema}.project_repos_languages limit 10")
                    result = conn.execute(preview_query)
                    result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            else:
                # raise an error
                raise ValueError("No data to write")
                row_count_this_run = 0
                row_count = 0
                result_df = pd.DataFrame()
                count_http_errors_github_api = {
                    'count_403_errors': 0,
                    'count_502_errors': 0
                }

        except Exception as e:
            context.log.warning(f"error: {e}")

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                "row_count_this_run": dg.MetadataValue.int(row_count_this_run),
                "count_http_403_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_403_errors']),
                "count_http_502_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_502_errors']),
            }
        )

    return _github_project_repos_languages_env_specific


# to accomodate multiple environments, we will use a factory function
def create_github_project_repos_commits_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="github_project_repos_commits",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
    )
    def _github_project_repos_commits_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"]  

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        # get the github personal access token
        gh_pat = github_api.get_client(config.key_name)

        def get_non_github_repo_commits(repo_url, repo_source):

            context.log.info(f"processing non-githubrepo: {repo_url}")

            # add a 0.25 second delay to avoid rate limiting
            # note: this is simplified solution but there are not many non-github repos
            time.sleep(0.5)

            if repo_source == "bitbucket":
                # Extract owner and repo_slug from the URL
                try:
                    parts = repo_url.rstrip('/').split('/')
                    owner = parts[-2]
                    repo_slug = parts[-1]
                    if '.' in repo_slug:
                        repo_slug = repo_slug.split('.')[0]
                except IndexError:
                    context.log.warning(f"Invalid Bitbucket URL format: {repo_url}")
                    return {'repo': repo_url, 'commit_count': None}

                try:
                    # Construct the correct Bitbucket API endpoint
                    api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}"

                    response = requests.get(api_url)
                    
                    # check if the response is successful
                    response.raise_for_status()

                    # use the response to get the commit count
                    commits_url = response.json()['links']['commits']['href']

                    # tell the api to return 100 commits per page
                    commits_url = f"{commits_url}?pagelen=100"

                    # get the commit count
                    commit_count = 0
                    while commits_url:
                        response = requests.get(commits_url)
                        if response.status_code == 200:
                            data = response.json()
                            commit_count += len(data.get("values", []))
                            commits_url = data.get("next")  # Get the next page URL
                        else:
                            context.log.warning(f"Error: {response.status_code}")
                            break  # Exit the loop on error

                    # Get the commit count from the 'size' field
                    return {'repo': repo_url, 'commit_count': commit_count}

                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from Bitbucket API: {e}")
                    return {'repo': repo_url, 'commit_count': None}
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}")
                    return {'repo': repo_url, 'commit_count': None}
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return {'repo': repo_url, 'commit_count': None}

            elif repo_source == "gitlab":
                try:
                    parts = repo_url.rstrip('/').split('/')
                    project_path = "/".join(parts[3:])
                    project_path_encoded = requests.utils.quote(project_path, safe='')
                except IndexError:
                    context.log.warning(f"Invalid GitLab URL format: {repo_url}")
                    return {'repo': repo_url, 'commit_count': None}

                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

                try:
                    response = requests.get(api_url)  # No headers needed for unauthenticated access
                    response.raise_for_status()

                    # now access commits endpoint
                    commits_url = f"https://gitlab.com/api/v4/projects/{response.json()['id']}/repository/commits?per_page=100"
                    commits = []
                    while commits_url:
                        try:
                            time.sleep(0.2)
                            response = requests.get(commits_url)
                            response.raise_for_status()

                            if response.status_code == 200:
                                commits.extend(response.json())
                                link_header = response.headers.get("Link")
                                if link_header:
                                    next_link = None
                                    links = link_header.split(",")
                                    for link in links:
                                        if 'rel="next"' in link:
                                            next_link = link.split(";")[0].strip()  # Remove extra spaces
                                            next_link = next_link.strip("<>").strip()  # Remove angle brackets
                                            break
                                    commits_url = next_link
                                else:
                                    commits_url = None
                            else:
                                context.log.warning(f"Error: {response.status_code}")
                                break
                        except requests.exceptions.RequestException as e:
                            context.log.warning(f"Error fetching data from GitLab API: {e}")
                            break
                    
                    # return the commit count data
                    if commits:
                        return {'repo': repo_url, 'commit_count': len(commits)}
                    else:
                        return {'repo': repo_url, 'commit_count': None}
                        
                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from GitLab API: {e}")
                    return {'repo': repo_url, 'commit_count': None}
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}") 
                    return {'repo': repo_url, 'commit_count': None}
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return {'repo': repo_url, 'commit_count': None}
            else:
                return {'repo': repo_url, 'commit_count': None}

        def get_github_repo_commits(repo_urls, gh_pat):
            """
            Queries the commits for a GitHub repository using the GraphQL API.

            Args:
                repo_urls: A list of GitHub repository URLs.

            Returns:
                A list of dictionaries, where each dictionary represents a repo and
                its commits data.  Returns an empty list if no repos are provided.
            """
            if not repo_urls:  # Handle empty input list
                return [], 0

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat}"}
            results = {}
            batch_size = 75  # Adjust as needed
            cpu_time_used = 0
            real_time_used = 0
            real_time_window = 60
            cpu_time_limit = 50
            count_403_errors = 0
            count_502_errors = 0
            batch_time_history = []

            for i in range(0, len(repo_urls), batch_size):
                context.log.info(f"processing batch: {i} - {i + batch_size}")
                start_time = time.time()
                batch = repo_urls[i:i + batch_size]
                processed_in_batch = set()  # Track successfully processed repos *within this batch*

                query = "query ("
                variables = {}

                for j, repo_url in enumerate(batch):
                    try:
                        parts = repo_url.rstrip('/').split('/')
                        owner = parts[-2]
                        name = parts[-1]
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format: {repo_url}")
                        # No need to append here, handle at the end of the batch loop
                        continue

                    query += f"$owner{j}: String!, $name{j}: String!,"
                    variables[f"owner{j}"] = owner
                    variables[f"name{j}"] = name

                query = query.rstrip(",")
                query += ") {\n"

                for j, repo_url in enumerate(batch):
                    query += f"""  repo{j}: repository(owner: $owner{j}, name: $name{j}) {{
                        defaultBranchRef {{
                            target {{
                                ... on Commit {{
                                    history {{
                                        totalCount
                                    }}
                                }}
                            }}
                        }}
                    }}\n"""

                query += "}"

                base_delay = 1
                max_delay = 60
                max_retries = 8

                for attempt in range(max_retries):
                    context.log.info(f"attempt: {attempt}")

                    try:
                        if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                            extra_delay = (cpu_time_used - cpu_time_limit) / 2
                            extra_delay = max(1, extra_delay)
                            context.log.info(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                            time.sleep(extra_delay)
                            context.log.info(f"resetting cpu_time_used and real_time_used to 0")
                            cpu_time_used = 0
                            real_time_used = 0
                            start_time = time.time()
                        elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info(f"real time limit reached without CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                            context.log.info(f"real time limit reached. CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info('cpu time limit not reached. Continuing...')

                        response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)

                        time_since_start = time.time() - start_time
                        context.log.info(f"time_since_start: {time_since_start:.2f} seconds")
                        time.sleep(1.7)  # Consistent delay

                        response.raise_for_status()
                        data = response.json()

                        if 'errors' in data:
                            context.log.info(f"Status Code: {response.status_code}")
                            context.log.info(" \nresource usage tracking:")
                            rate_limit_info = {
                                'remaining': response.headers.get('x-ratelimit-remaining'),
                                'used': response.headers.get('x-ratelimit-used'),
                                'reset': response.headers.get('x-ratelimit-reset'),
                                'retry_after': response.headers.get('retry-after')
                            }
                            context.log.info(f"Rate Limit Info: {rate_limit_info}\n")

                            for error in data['errors']:
                                if error['type'] == 'RATE_LIMITED':
                                    reset_at = response.headers.get('X-RateLimit-Reset')
                                    if reset_at:
                                        delay = int(reset_at) - int(time.time()) + 1
                                        delay = max(1, delay)
                                        delay = min(delay, max_delay)
                                        context.log.info(f"Rate limited.  Waiting for {delay} seconds...")
                                        time.sleep(delay)
                                        continue  # Retry the entire batch

                                    else:
                                        context.log.info(f"GraphQL Error: {error}")  # Print all the errors.

                        if 'data' in data:
                            for j, repo_url in enumerate(batch):
                                if repo_url in processed_in_batch:  # CRUCIAL CHECK
                                    continue  # Skip if already processed

                                repo_data = data['data'].get(f'repo{j}')
                                if repo_data and repo_data['defaultBranchRef'] and repo_data['defaultBranchRef']['target']:
                                    total_count = repo_data['defaultBranchRef']['target']['history']['totalCount']
                                    results[repo_url] = total_count
                                    processed_in_batch.add(repo_url)  # Mark as processed
                                else:
                                    # Handle cases where the repo is empty, doesn't have a default branch, or is inaccessible
                                    results[repo_url] = None
                                    # Don't append here; handle missing data at the end
                        break  # Exit retry loop if successful

                    except requests.exceptions.RequestException as e:
                        context.log.warning(f"there was a request exception on attempt: {attempt}\n")
                        context.log.warning(f"procesing batch: {batch}\n")
                        context.log.warning(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        context.log.warning(" \nresource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        context.log.warning(f"Rate Limit Info: {rate_limit_info}\n")

                        context.log.warning(f"the error is: {e}\n")
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached or unrecoverable error for batch. Giving up.")
                            # Don't append here; handle failures at the end
                            break

                        if isinstance(e, requests.exceptions.HTTPError):
                            if e.response.status_code in (502, 504):
                                count_502_errors += 1
                                context.log.warning(f"This process has generated {count_502_errors} 502/504 errors in total.")
                                delay = 1
                                context.log.warning(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                            elif e.response.status_code in (403, 429):
                                count_403_errors += 1
                                context.log.warning(f"This process has generated {count_403_errors} 403/429 errors in total.")
                                retry_after = response.headers.get('Retry-After')
                                if retry_after:
                                    delay = int(retry_after)
                                    context.log.warning(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue
                                else:
                                    delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                    context.log.warning(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                    time.sleep(delay)
                                    continue
                        else:
                            delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                            context.log.warning(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)

                    except KeyError as e:
                        context.log.warning(f"KeyError: {e}. Response: {data}")
                        # Don't append here; handle errors at the end
                        break
                    except Exception as e:
                        context.log.warning(f"An unexpected error occurred: {e}")
                        # Don't append here; handle errors at the end
                        break

                # Handle any repos that failed *all* retries (or were invalid URLs)
                for repo_url in batch:
                    if repo_url not in processed_in_batch:
                        results[repo_url] = None
                        context.log.warning(f"adding repo to results after max retries, or was invalid url: {repo_url}")

                end_time = time.time()
                batch_time = end_time - start_time
                cpu_time_used += time_since_start
                real_time_used += batch_time
                batch_time_history.append(batch_time)
                if batch_time_history and len(batch_time_history) > 10:
                    context.log.info(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
                context.log.info(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
                context.log.info(f"time taken to process batch {i}: {batch_time:.2f} seconds")
                context.log.info(f"Total CPU time used: {cpu_time_used:.2f} seconds")
                context.log.info(f"Total real time used: {real_time_used:.2f} seconds")

            return results, {"count_403_errors": count_403_errors, "count_502_errors": count_502_errors}

        # Execute the query
        with cloud_sql_engine.connect() as conn:

            # query the latest_distinct_project_repos table to get the distinct repo list
            result = conn.execute(
                text(f"select repo, repo_source from {clean_schema}.latest_active_distinct_project_repos where is_active = true")
            )
            repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Filter for GitHub URLs
        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()
        # filter for non-github repos urls
        non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

        if len(github_urls) > 0:
            context.log.info(f"number of github urls: {len(github_urls)}")
            
            results = get_github_repo_commits(github_urls, gh_pat)

            if results:
                github_results = results[0]
                count_http_errors_github_api = results[1]

                # write results to pandas dataframe
                github_results_df = pd.DataFrame(github_results.items(), columns=['repo', 'commit_count'])
        else:
            github_results_df = pd.DataFrame()
            count_http_errors_github_api = {'count_403_errors': 0, 'count_502_errors': 0}

        # if non_github_urls is not empty, get fork count
        if not non_github_results_df.empty:
            context.log.info("found non-github repos. Getting repo commit data...")
            context.log.info(f"number of non-github urls: {non_github_results_df.shape[0]}")
            # Use list comprehension to get data for each non-github repo
            commit_data = [
                get_non_github_repo_commits(row['repo'], row['repo_source'])
                for _, row in non_github_results_df.iterrows()  # Or use itertuples for slight improvement
            ]

            # create a df
            if commit_data:
                non_github_results_df = pd.DataFrame(commit_data)
            else:
                non_github_results_df = pd.DataFrame()

        # append non_github_urls to unpacked_df
        if (not github_results_df.empty and not non_github_results_df.empty):
            # Both DataFrames are not empty, so concatenate them
            all_repos_df = pd.concat([github_results_df, non_github_results_df], ignore_index=True)
        elif not github_results_df.empty:
            # unpacked_df is not empty, use it as results_df
            all_repos_df = github_results_df
        elif not non_github_results_df.empty:
            # non_github_results_df is not empty, use it as results_df
            all_repos_df = non_github_results_df
        else:
            # Both DataFrames are empty
            all_repos_df = pd.DataFrame()  # Create an empty DataFrame

        # pandas deduplication
        if not all_repos_df.empty:  # Only deduplicate if there's data
            # all_repos_df.drop_duplicates(subset=['repo', 'language_name', 'repo_languages_total_bytes'], keep='first', inplace=True)
            # Add data_timestamp 
            all_repos_df['data_timestamp'] = pd.Timestamp.now()

            # Cast the count column to integer *before* writing to the database; fill na with 0
            all_repos_df['commit_count'] = all_repos_df['commit_count'].fillna(0).astype(int)

        # write results to database
        # wrap in try except
        try:
            if not all_repos_df.empty:
                all_repos_df.to_sql('project_repos_commit_count', cloud_sql_engine, if_exists='append', index=False, schema=raw_schema)

                # create variable to store the count of rows written to the database
                row_count_this_run = all_repos_df.shape[0]
            else:
                # raise an error
                raise ValueError("No data to write")
                row_count_this_run = 0

            with cloud_sql_engine.connect() as conn:
                # capture asset metadata
                preview_query = text(f"select count(*) from {raw_schema}.project_repos_commit_count")
                result = conn.execute(preview_query)
                # Fetch all rows into a list of tuples
                row_count = result.fetchone()[0]

                preview_query = text(f"select * from {raw_schema}.project_repos_commit_count limit 10")
                result = conn.execute(preview_query)
                result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            context.log.warning(f"error: {e}")

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                "row_count_this_run": dg.MetadataValue.int(row_count_this_run),
                "count_http_403_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_403_errors']),
                "count_http_502_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_502_errors']),
            }
        )

    return _github_project_repos_commits_env_specific


# define the asset that gets the watcher count for a repo
# to accomodate multiple environments, we will use a factory function
def create_github_project_repos_watcher_count_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="github_project_repos_watcher_count",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
    )
    def _github_project_repos_watcher_count_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"]  

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        # get the github personal access token
        gh_pat = github_api.get_client(config.key_name)

        def get_non_github_repo_watcher_count(repo_url, repo_source):

            context.log.info(f"processing non-githubrepo: {repo_url}")

            # add a 1 second delay to avoid rate limiting
            # note: this is simplified solution but there are not many non-github repos
            time.sleep(0.5)

            if repo_source == "bitbucket":
                # Extract owner and repo_slug from the URL
                try:
                    parts = repo_url.rstrip('/').split('/')
                    owner = parts[-2]
                    repo_slug = parts[-1]
                    if '.' in repo_slug:
                        repo_slug = repo_slug.split('.')[0]
                except IndexError:
                    context.log.warning(f"Invalid Bitbucket URL format: {repo_url}")
                    return None

                try:
                    # Construct the correct Bitbucket API endpoint
                    api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}"

                    response = requests.get(api_url)

                    # check if the response is successful
                    response.raise_for_status()

                    watchers_url = response.json()['links']['watchers']['href']
                    watchers_response = requests.get(watchers_url)
                    watchers_response.raise_for_status()
                    watchers_data = watchers_response.json()

                    # Get the watcher count from the 'size' field
                    return watchers_data['size']

                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from Bitbucket API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}")
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None

            elif repo_source == "gitlab":
                try:
                    parts = repo_url.rstrip('/').split('/')
                    project_path = "/".join(parts[3:])
                    project_path_encoded = requests.utils.quote(project_path, safe='')
                except IndexError:
                    context.log.warning(f"Invalid GitLab URL format: {repo_url}")
                    return {'repo': repo_url, 'watcher_count': None}

                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

                try:
                    response = requests.get(api_url)  # No headers needed for unauthenticated access
                    response.raise_for_status()

                    # now access watchers endpoint
                    watchers_url = f"https://gitlab.com/api/v4/projects/{response.json()['id']}/users?subscribed=true?per_page=100"
                    watchers = []
                    while watchers_url:
                        try:
                            time.sleep(0.2)
                            response = requests.get(watchers_url)
                            response.raise_for_status()

                            if response.status_code == 200:
                                watchers.extend(response.json())
                                link_header = response.headers.get("Link")
                                if link_header:
                                    next_link = None
                                    links = link_header.split(",")
                                    for link in links:
                                        if 'rel="next"' in link:
                                            next_link = link.split(";")[0].strip()  # Remove extra spaces
                                            next_link = next_link.strip("<>").strip()  # Remove angle brackets
                                            break
                                    watchers_url = next_link
                                else:
                                    watchers_url = None
                            else:
                                context.log.warning(f"Error: {response.status_code}")
                                break
                        except requests.exceptions.RequestException as e:
                            context.log.warning(f"Error fetching data from GitLab API: {e}")
                            break
                    
                    # return the watcher count data
                    if watchers:
                        return len(watchers)
                    else:
                        return None
                        
                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from GitLab API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}") 
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None
            else:
                return None

        def get_github_repo_watcher_count(repo_urls, gh_pat):
            """
            Queries the watcher count for a GitHub repository using the GraphQL API.

            Args:
                repo_urls: A list of GitHub repository URLs.

            Returns:
                A dictionary mapping each repository URL to the watcher count.
            """

            if not repo_urls:  # Handle empty input list
                return [], 0

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat}"}
            results = {}  # Store results: {url: watcher_count}
            batch_size = 120  # Adjust as needed
            cpu_time_used = 0
            real_time_used = 0
            real_time_window = 60
            cpu_time_limit = 50
            count_502_errors = 0
            count_403_errors = 0
            batch_time_history = []

            for i in range(0, len(repo_urls), batch_size):
                context.log.info(f"processing batch: {i} - {i + batch_size}")
                # calculate the time it takes to process the batch
                start_time = time.time()
                batch = repo_urls[i:i + batch_size]
                processed_in_batch = set()  # Track successfully processed repos *within this batch*
                query = "query ("  # Start the query definition
                variables = {}

                # 1. Declare variables in the query definition
                for j, repo_url in enumerate(batch):
                    try:
                        parts = repo_url.rstrip('/').split('/')
                        owner = parts[-2]
                        name = parts[-1]
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format: {repo_url}")
                        continue

                    query += f"$owner{j}: String!, $name{j}: String!,"  # Declare variables
                    variables[f"owner{j}"] = owner
                    variables[f"name{j}"] = name

                query = query.rstrip(",")  # Remove trailing comma
                query += ") {\n"  # Close the variable declaration

                # 2. Construct the query body (using the declared variables)
                for j, repo_url in enumerate(batch):
                    query += f"""  repo{j}: repository(owner: $owner{j}, name: $name{j}) {{
                            watchers {{
                                totalCount
                            }}
                    }}\n"""

                query += "}"

                base_delay = 1
                max_delay = 60
                max_retries = 8

                for attempt in range(max_retries):
                    context.log.info(f"attempt: {attempt}")
                    
                    try:
                        if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                            extra_delay = (cpu_time_used - cpu_time_limit) / 2
                            extra_delay = max(1, extra_delay)
                            context.log.info(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                            time.sleep(extra_delay)
                            context.log.info(f"resetting cpu_time_used and real_time_used to 0")
                            cpu_time_used = 0
                            real_time_used = 0
                            start_time = time.time()
                        elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info(f"real time limit reached without CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                            context.log.info(f"real time limit reached. CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info('cpu time limit not reached. Continuing...')

                        response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                        time_since_start = time.time() - start_time
                        context.log.info(f"time_since_start: {time_since_start:.2f} seconds")
                        time.sleep(2)  # Consistent delay

                        # use raise for status to catch errors
                        response.raise_for_status()
                        data = response.json()

                        if 'errors' in data:
                            context.log.info(f"Status Code: {response.status_code}")
                            # Extract rate limit information from headers
                            context.log.info(" \n resource usage tracking:")
                            rate_limit_info = {
                                'remaining': response.headers.get('x-ratelimit-remaining'),
                                'used': response.headers.get('x-ratelimit-used'),
                                'reset': response.headers.get('x-ratelimit-reset'),
                                'retry_after': response.headers.get('retry-after')
                            }
                            context.log.info(f"Rate Limit Info: {rate_limit_info}\n")

                            for error in data['errors']:
                                if error['type'] == 'RATE_LIMITED':
                                    reset_at = response.headers.get('X-RateLimit-Reset')
                                    if reset_at:
                                        delay = int(reset_at) - int(time.time()) + 1
                                        delay = max(1, delay)
                                        delay = min(delay, max_delay)
                                        context.log.warning(f"Rate limited.  Waiting for {delay} seconds...")
                                        time.sleep(delay)
                                        continue  # Retry the entire batch
                                else:
                                    context.log.warning(f"GraphQL Error: {error}") #Print all the errors.

                        # write the url and watcher count to the database
                        if 'data' in data:
                            for j, repo_url in enumerate(batch):
                                if repo_url in processed_in_batch:  # CRUCIAL CHECK
                                    continue  # Skip if already processed
                                repo_data = data['data'].get(f'repo{j}')
                                if repo_data:
                                    results[repo_url] = repo_data['watchers']['totalCount']
                                    processed_in_batch.add(repo_url)  # Mark as processed
                                else:
                                    context.log.warning(f"repo_data is empty for repo: {repo_url}\n")
                        break

                    except requests.exceptions.RequestException as e:
                        context.log.warning(f"there was a request exception on attempt: {attempt}\n")
                        context.log.warning(f"procesing batch: {batch}\n")
                        context.log.warning(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        context.log.warning(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        context.log.warning(f"Rate Limit Info: {rate_limit_info}\n")

                        context.log.warning(f"the error is: {e}\n")
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached or unrecoverable error for batch. Giving up.")
                            break
                        # --- Rate Limit Handling (REST API style - for 403/429) ---
                        if isinstance(e, requests.exceptions.HTTPError):
                            if e.response.status_code in (502, 504):
                                count_502_errors += 1
                                context.log.warning(f"This process has generated {count_502_errors} 502/504 errors in total.")
                                delay = 1
                                context.log.warning(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                            elif e.response.status_code in (403, 429):
                                count_403_errors += 1
                                context.log.warning(f"This process has generated {count_403_errors} 403/429 errors in total.")
                                retry_after = response.headers.get('Retry-After')
                                if retry_after:
                                    delay = int(retry_after)
                                    context.log.warning(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue
                                else:
                                    delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                    context.log.warning(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                    time.sleep(delay)
                                    continue
                        else:
                            delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                            context.log.warning(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)

                    except KeyError as e:
                        context.log.warning(f"KeyError: {e}. Response: {data}")
                        # Don't append here; handle errors at the end
                        break
                    except Exception as e:
                        context.log.warning(f"An unexpected error occurred: {e}")
                        # Don't append here; handle errors at the end
                        break

                # Handle any repos that failed *all* retries (or were invalid URLs)
                for repo_url in batch:
                    if repo_url not in processed_in_batch:
                        results[repo_url] = None
                        context.log.warning(f"adding repo to results after max retries, or was invalid url: {repo_url}")

                end_time = time.time()
                batch_time = end_time - start_time
                cpu_time_used += time_since_start
                real_time_used += batch_time
                batch_time_history.append(batch_time)
                if batch_time_history and len(batch_time_history) > 10:
                    context.log.info(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
                context.log.info(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
                context.log.info(f"time taken to process batch {i}: {batch_time:.2f} seconds")
                context.log.info(f"Total CPU time used: {cpu_time_used:.2f} seconds")
                context.log.info(f"Total real time used: {real_time_used:.2f} seconds")

            return results, {
                'count_403_errors': count_403_errors,
                'count_502_errors': count_502_errors
            }

        # Execute the query
        with cloud_sql_engine.connect() as conn:

            # query the latest_distinct_project_repos table to get the distinct repo list
            result = conn.execute(
                text(f"""select repo, repo_source from {clean_schema}.latest_active_distinct_project_repos where is_active = true""")
                    )
            repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Filter for GitHub URLs
        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

        context.log.info(f"number of github urls: {len(github_urls)}")

        # check if github_urls is not empty
        if github_urls:
            results = get_github_repo_watcher_count(github_urls, gh_pat)

            github_results = results[0]
            count_http_errors_github_api = results[1]

            # write results to pandas dataframe
            results_df = pd.DataFrame(github_results.items(), columns=['repo', 'watcher_count'])
        else:
            results_df = pd.DataFrame(columns=['repo', 'watcher_count'])

        # now get non-github repos urls
        non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

        # if non_github_urls is not empty, get watcher count
        if not non_github_results_df.empty:
            context.log.info("found non-github repos. Getting repo watcher count...")
            # apply distinct_repo_df['repo'] to get watcher count
            non_github_results_df['watcher_count'] = non_github_results_df.apply(
                lambda row: get_non_github_repo_watcher_count(row['repo'], row['repo_source']), axis=1
            )

            # drop the repo_source column
            non_github_results_df = non_github_results_df.drop(columns=['repo_source'])

            # append non_github_urls to results_df
            results_df = pd.concat([results_df, non_github_results_df])

        # check if results_df is not empty
        if not results_df.empty:
            # add unix datetime column
            results_df['data_timestamp'] = pd.Timestamp.now()

            # write results to database
            results_df.to_sql('project_repos_watcher_count', cloud_sql_engine, if_exists='append', index=False, schema=raw_schema)

            with cloud_sql_engine.connect() as conn:
                # capture asset metadata
                preview_query = text(f"select count(*) from {raw_schema}.project_repos_watcher_count")
                result = conn.execute(preview_query)
                # Fetch all rows into a list of tuples
                row_count = result.fetchone()[0]

                preview_query = text(f"select * from {raw_schema}.project_repos_watcher_count limit 10")
                result = conn.execute(preview_query)
                result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

            return dg.MaterializeResult(
                metadata={
                    "row_count": dg.MetadataValue.int(row_count),
                    "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                    "count_403_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_403_errors']),
                    "count_502_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_502_errors'])
                }
            )
        else:
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

    return _github_project_repos_watcher_count_env_specific


# define the asset that gets the boolean isFork for a repo
# to accomodate multiple environments, we will use a factory function
def create_github_project_repos_is_fork_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="github_project_repos_is_fork",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
    )
    def _github_project_repos_is_fork_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        # get the github personal access token
        gh_pat = github_api.get_client(config.key_name)

        def get_non_github_repo_is_fork(repo_url, repo_source):

            context.log.info(f"processing non-githubrepo: {repo_url}")

            # add a 1 second delay to avoid rate limiting
            # note: this is simplified solution but there are not many non-github repos
            time.sleep(0.5)

            if repo_source == "bitbucket":
                # Extract owner and repo_slug from the URL
                try:
                    parts = repo_url.rstrip('/').split('/')
                    owner = parts[-2]
                    repo_slug = parts[-1]
                    if '.' in repo_slug:
                        repo_slug = repo_slug.split('.')[0]
                except IndexError:
                    context.log.warning(f"Invalid Bitbucket URL format: {repo_url}")
                    return None

                try:
                    # Construct the correct Bitbucket API endpoint
                    api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}"

                    response = requests.get(api_url)

                    # check if the response is successful
                    response.raise_for_status()

                    # check if parent key is included in the response
                    if 'parent' in response.json():
                        # check if parent key is not None
                        if response.json()['parent'] is not None:
                            return True
                        else:
                            return False
                    else:
                        return False
                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from Bitbucket API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}")
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None

            elif repo_source == "gitlab":
                try:
                    parts = repo_url.rstrip('/').split('/')
                    project_path = "/".join(parts[3:])
                    project_path_encoded = requests.utils.quote(project_path, safe='')
                except IndexError:
                    context.log.warning(f"Invalid GitLab URL format: {repo_url}")
                    return {'repo': repo_url, 'watcher_count': None}

                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

                try:
                    response = requests.get(api_url)  # No headers needed for unauthenticated access
                    response.raise_for_status()

                    # now access check if the forked_from_project key is included in the response endpoint
                    if 'forked_from_project' in response.json():
                        # check if the forked_from_project key is not None
                        if response.json()['forked_from_project'] is not None:
                            return True
                        else:
                            return False
                    else:
                        return False

                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from GitLab API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}")
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None
            else:
                return None

        def get_github_repo_is_fork(repo_urls, gh_pat):
            """
            Queries the isFork count for a GitHub repository using the GraphQL API.

            Args:
                repo_urls: A list of GitHub repository URLs.

            Returns:
                A dictionary mapping each repository URL to the isFork count.
            """

            if not repo_urls:  # Handle empty input list
                return [], 0

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat}"}
            results = {}  # Store results: {url: watcher_count}
            batch_size = 120  # Adjust as needed
            cpu_time_used = 0
            real_time_used = 0
            real_time_window = 60
            cpu_time_limit = 50
            count_502_errors = 0
            count_403_errors = 0
            batch_time_history = []

            for i in range(0, len(repo_urls), batch_size):
                context.log.info(f"processing batch: {i} - {i + batch_size}")
                # calculate the time it takes to process the batch
                start_time = time.time()
                batch = repo_urls[i:i + batch_size]
                processed_in_batch = set()  # Track successfully processed repos *within this batch*
                query = "query ("  # Start the query definition
                variables = {}

                # 1. Declare variables in the query definition
                for j, repo_url in enumerate(batch):
                    try:
                        parts = repo_url.rstrip('/').split('/')
                        owner = parts[-2]
                        name = parts[-1]
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format: {repo_url}")
                        continue

                    query += f"$owner{j}: String!, $name{j}: String!,"  # Declare variables
                    variables[f"owner{j}"] = owner
                    variables[f"name{j}"] = name

                query = query.rstrip(",")  # Remove trailing comma
                query += ") {\n"  # Close the variable declaration

                # 2. Construct the query body (using the declared variables)
                for j, repo_url in enumerate(batch):
                    query += f"""  repo{j}: repository(owner: $owner{j}, name: $name{j}) {{
                            isFork
                    }}\n"""

                query += "}"

                base_delay = 1
                max_delay = 60
                max_retries = 8

                for attempt in range(max_retries):
                    context.log.info(f"attempt: {attempt}")
                    
                    try:
                        if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                            extra_delay = (cpu_time_used - cpu_time_limit) / 2
                            extra_delay = max(1, extra_delay)
                            context.log.info(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                            time.sleep(extra_delay)
                            context.log.info(f"resetting cpu_time_used and real_time_used to 0")
                            cpu_time_used = 0
                            real_time_used = 0
                            start_time = time.time()
                        elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info(f"real time limit reached without CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                            context.log.info(f"real time limit reached. CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info('cpu time limit not reached. Continuing...')

                        response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                        time_since_start = time.time() - start_time
                        context.log.info(f"time_since_start: {time_since_start:.2f} seconds")
                        time.sleep(3)  # Consistent delay

                        # use raise for status to catch errors
                        response.raise_for_status()
                        data = response.json()

                        if 'errors' in data:
                            context.log.warning(f"Status Code: {response.status_code}")
                            # Extract rate limit information from headers
                            context.log.info(" \n resource usage tracking:")
                            rate_limit_info = {
                                'remaining': response.headers.get('x-ratelimit-remaining'),
                                'used': response.headers.get('x-ratelimit-used'),
                                'reset': response.headers.get('x-ratelimit-reset'),
                                'retry_after': response.headers.get('retry-after')
                            }
                            context.log.info(f"Rate Limit Info: {rate_limit_info}\n")

                            for error in data['errors']:
                                if error['type'] == 'RATE_LIMITED':
                                    reset_at = response.headers.get('X-RateLimit-Reset')
                                    if reset_at:
                                        delay = int(reset_at) - int(time.time()) + 1
                                        delay = max(1, delay)
                                        delay = min(delay, max_delay)
                                        context.log.info(f"Rate limited.  Waiting for {delay} seconds...")
                                        time.sleep(delay)
                                        continue  # Retry the entire batch
                                else:
                                    context.log.warning(f"GraphQL Error: {error}") #Print all the errors.

                        # write the url and isFork to the database
                        if 'data' in data:
                            for j, repo_url in enumerate(batch):
                                if repo_url in processed_in_batch:  # CRUCIAL CHECK
                                    continue  # Skip if already processed
                                repo_data = data['data'].get(f'repo{j}')
                                if repo_data:
                                    results[repo_url] = repo_data['isFork']
                                    processed_in_batch.add(repo_url)  # Mark as processed
                                else:
                                    context.log.warning(f"repo_data is empty for repo: {repo_url}\n")
                        break

                    except requests.exceptions.RequestException as e:
                        context.log.warning(f"there was a request exception on attempt: {attempt}\n")
                        context.log.warning(f"procesing batch: {batch}\n")
                        context.log.warning(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        context.log.warning(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        context.log.warning(f"Rate Limit Info: {rate_limit_info}\n")

                        context.log.warning(f"the error is: {e}\n")
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached or unrecoverable error for batch. Giving up.")
                            break
                        # --- Rate Limit Handling (REST API style - for 403/429) ---
                        if isinstance(e, requests.exceptions.HTTPError):
                            if e.response.status_code in (502, 504):
                                count_502_errors += 1
                                context.log.warning(f"This process has generated {count_502_errors} 502/504 errors in total.")
                                delay = 1
                                context.log.warning(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                            elif e.response.status_code in (403, 429):
                                count_403_errors += 1
                                context.log.warning(f"This process has generated {count_403_errors} 403/429 errors in total.")
                                retry_after = response.headers.get('Retry-After')
                                if retry_after:
                                    delay = int(retry_after)
                                    context.log.warning(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue
                                else:
                                    delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                    context.log.warning(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                    time.sleep(delay)
                                    continue
                        else:
                            delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                            context.log.warning(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)

                    except KeyError as e:
                        context.log.warning(f"KeyError: {e}. Response: {data}")
                        # Don't append here; handle errors at the end
                        break
                    except Exception as e:
                        context.log.warning(f"An unexpected error occurred: {e}")
                        # Don't append here; handle errors at the end
                        break

                # Handle any repos that failed *all* retries (or were invalid URLs)
                for repo_url in batch:
                    if repo_url not in processed_in_batch:
                        results[repo_url] = None
                        context.log.warning(f"adding repo to results after max retries, or was invalid url: {repo_url}")

                end_time = time.time()
                batch_time = end_time - start_time
                cpu_time_used += time_since_start
                real_time_used += batch_time
                batch_time_history.append(batch_time)
                if batch_time_history and len(batch_time_history) > 10:
                    context.log.info(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
                context.log.info(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
                context.log.info(f"time taken to process batch {i}: {batch_time:.2f} seconds")
                context.log.info(f"Total CPU time used: {cpu_time_used:.2f} seconds")
                context.log.info(f"Total real time used: {real_time_used:.2f} seconds")

            return results, {
                'count_403_errors': count_403_errors,
                'count_502_errors': count_502_errors
            }

        # Execute the query
        with cloud_sql_engine.connect() as conn:

            # query the latest_distinct_project_repos table to get the distinct repo list
            result = conn.execute(
                text(f"""select repo, repo_source from {clean_schema}.latest_active_distinct_project_repos where is_active = true""")
                    )
            repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Filter for GitHub URLs
        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

        context.log.info(f"number of github urls: {len(github_urls)}")

        # check if github_urls is not empty
        if github_urls:
            results = get_github_repo_is_fork(github_urls, gh_pat)

            github_results = results[0]
            count_http_errors_github_api = results[1]

            # write results to pandas dataframe
            results_df = pd.DataFrame(github_results.items(), columns=['repo', 'is_fork'])
        else:
            results_df = pd.DataFrame(columns=['repo', 'is_fork'])

        # now get non-github repos urls
        non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

        # if non_github_urls is not empty, get is_fork status
        if not non_github_results_df.empty:
            context.log.info("found non-github repos. Getting repo isFork...")
            # apply distinct_repo_df['repo'] to get watcher count
            non_github_results_df['is_fork'] = non_github_results_df.apply(
                lambda row: get_non_github_repo_is_fork(row['repo'], row['repo_source']), axis=1
            )

            # drop the repo_source column
            non_github_results_df = non_github_results_df.drop(columns=['repo_source'])

            # append non_github_urls to results_df
            results_df = pd.concat([results_df, non_github_results_df])

        # check if results_df is not empty
        if not results_df.empty:
            context.log.info("Starting cleanup for 'is_fork' column...")
            # Ensure the column exists before proceeding
            if 'is_fork' in results_df.columns:
                # Define allowed boolean values (True and False)
                allowed_bools = [True, False]

                # Create a mask to identify rows where 'is_fork' is:
                # 1. NOT True
                # 2. NOT False
                # 3. NOT Null (isna() handles None and np.nan)
                mask_invalid = ~results_df['is_fork'].isin(allowed_bools) & ~results_df['is_fork'].isna()

                # Check if any invalid values were found
                if mask_invalid.any():
                    num_invalid = mask_invalid.sum()
                    context.log.warning(f"Found {num_invalid} non-boolean/non-null values in 'is_fork' column. Converting to Null.")
                    # Log the actual invalid values found for debugging
                    invalid_values_found = results_df.loc[mask_invalid, 'is_fork'].unique()
                    context.log.warning(f"First 25 invalid values found: {invalid_values_found[:25]}")

                    # Set invalid values to np.nan (which pandas handles as Null)
                    results_df.loc[mask_invalid, 'is_fork'] = np.nan

                # Explicitly convert the column to pandas nullable boolean dtype
                # This helps ensure consistency and correct handling of NA/NaN by to_sql.
                try:
                    # Before converting, fill NaN with None if the target SQL type doesn't handle NaN well directly
                    # Although SQLAlchemy usually handles np.nan -> NULL correctly for boolean
                    results_df['is_fork'] = results_df['is_fork'].where(pd.notna(results_df['is_fork']), None)
                    results_df['is_fork'] = results_df['is_fork'].astype('boolean') # Use 'boolean' (Pandas NA) not 'bool'
                    context.log.info("Successfully converted 'is_fork' column to pandas nullable boolean type.")
                except Exception as e:
                    # Log error if conversion fails, but might proceed if np.nan handling is okay
                    context.log.warning(f"Warning: Could not convert 'is_fork' to pandas nullable boolean type: {e}. Proceeding...")

            else:
                context.log.error("Warning: 'is_fork' column not found in results_df. Exiting.")
                raise Exception("'is_fork' column not found in results_df. Exiting.")

            # add unix datetime column
            results_df['data_timestamp'] = pd.Timestamp.now()

            # write results to database
            results_df.to_sql('project_repos_is_fork', cloud_sql_engine, if_exists='append', index=False, schema=raw_schema)

            with cloud_sql_engine.connect() as conn:
                # capture asset metadata
                preview_query = text(f"select count(*) from {raw_schema}.project_repos_is_fork")
                result = conn.execute(preview_query)
                # Fetch all rows into a list of tuples
                row_count = result.fetchone()[0]

                preview_query = text(f"select * from {raw_schema}.project_repos_is_fork limit 10")
                result = conn.execute(preview_query)
                result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

            return dg.MaterializeResult(
                metadata={
                    "row_count": dg.MetadataValue.int(row_count),
                    "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                    "count_403_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_403_errors']),
                    "count_502_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_502_errors'])
                }
            )
        else:
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

    return _github_project_repos_is_fork_env_specific


# define the asset that gets the repo description for a repo
# to accomodate multiple environments, we will use a factory function
def create_project_repos_description_asset(env_prefix: str):
    """
    Factory function to create the project_repos_description asset
    with an environment-specific key_prefix.
    """
    @dg.asset(
        key_prefix=env_prefix,  # <<< This is the key change for namespacing
        name="project_repos_description", # This is the base name of the asset
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion", # Group name
        tags={"github_api": "True"},
    )
    def _project_repos_description_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        # get the github personal access token
        gh_pat = github_api.get_client(config.key_name)

        def get_non_github_repo_description(repo_url, repo_source):

            context.log.info(f"processing non-githubrepo: {repo_url}")

            # add a 1 second delay to avoid rate limiting
            # note: this is simplified solution but there are not many non-github repos
            time.sleep(0.5)

            if repo_source == "bitbucket":
                # Extract owner and repo_slug from the URL
                try:
                    parts = repo_url.rstrip('/').split('/')
                    owner = parts[-2]
                    repo_slug = parts[-1]
                    if '.' in repo_slug:
                        repo_slug = repo_slug.split('.')[0]
                except IndexError:
                    context.log.warning(f"Invalid Bitbucket URL format: {repo_url}")
                    return None

                try:
                    # Construct the correct Bitbucket API endpoint
                    api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}"

                    response = requests.get(api_url)

                    # check if the response is successful
                    response.raise_for_status()

                    repo_description = response['description']

                    # Get the repo description field
                    return repo_description

                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from Bitbucket API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}")
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None

            elif repo_source == "gitlab":
                try:
                    parts = repo_url.rstrip('/').split('/')
                    project_path = "/".join(parts[3:])
                    project_path_encoded = requests.utils.quote(project_path, safe='')
                except IndexError:
                    context.log.warning(f"Invalid GitLab URL format: {repo_url}")
                    return None

                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

                try:
                    response = requests.get(api_url)  # No headers needed for unauthenticated access
                    response.raise_for_status()

                    # return the description
                    return response.json()['description']
                except requests.exceptions.RequestException as e:
                    context.log.warning(f"Error fetching data from GitLab API: {e}")
                    return None
                except KeyError as e:
                    context.log.warning(f"Error: missing key in response.  Key: {e}") 
                    return None
                except Exception as e:
                    context.log.warning(f"An unexpected error has occurred: {e}")
                    return None
            else:
                return None

        def get_github_repo_description(repo_urls, gh_pat):
            """
            Queries the repo description for a GitHub repository using the GraphQL API.

            Args:
                repo_urls: A list of GitHub repository URLs.

            Returns:
                A dictionary mapping each repository URL to the repo description.
            """

            if not repo_urls:  # Handle empty input list
                return [], 0

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat}"}
            results = {}  # Store results: {url: repo_description}
            batch_size = 150  # Adjust as needed
            cpu_time_used = 0
            real_time_used = 0
            real_time_window = 60
            cpu_time_limit = 50
            count_403_errors = 0
            count_502_errors = 0
            batch_time_history = []

            for i in range(0, len(repo_urls), batch_size):
                context.log.info(f"processing batch: {i} - {i + batch_size}")
                # calculate the time it takes to process the batch
                start_time = time.time()
                batch = repo_urls[i:i + batch_size]
                processed_in_batch = set()  # Track successfully processed repos *within this batch*
                query = "query ("  # Start the query definition
                variables = {}

                # 1. Declare variables in the query definition
                for j, repo_url in enumerate(batch):
                    try:
                        parts = repo_url.rstrip('/').split('/')
                        owner = parts[-2]
                        name = parts[-1]
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format: {repo_url}")
                        # don't return here, return errors at end of batch
                        continue

                    query += f"$owner{j}: String!, $name{j}: String!,"  # Declare variables
                    variables[f"owner{j}"] = owner
                    variables[f"name{j}"] = name

                query = query.rstrip(",")  # Remove trailing comma
                query += ") {\n"  # Close the variable declaration

                # 2. Construct the query body (using the declared variables)
                for j, repo_url in enumerate(batch):
                    query += f"""  repo{j}: repository(owner: $owner{j}, name: $name{j}) {{
                        description
                    }}\n"""

                query += "}"

                base_delay = 1
                max_delay = 60
                max_retries = 8

                for attempt in range(max_retries):
                    context.log.info(f"attempt: {attempt}")
                    
                    try:
                        if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                            extra_delay = (cpu_time_used - cpu_time_limit) / 2
                            extra_delay = max(1, extra_delay)
                            context.log.info(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                            time.sleep(extra_delay)
                            context.log.info(f"resetting cpu_time_used and real_time_used to 0")
                            cpu_time_used = 0
                            real_time_used = 0
                            start_time = time.time()
                        elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info(f"real time limit reached without CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                            context.log.info(f"real time limit reached. CPU time limit reached. Resetting counts.")
                            cpu_time_used = 0
                            real_time_used = 0
                        elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                            context.log.info('cpu time limit not reached. Continuing...')

                        response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)

                        time_since_start = time.time() - start_time
                        context.log.info(f"time_since_start: {time_since_start:.2f} seconds")
                        time.sleep(3)  # Consistent delay
                        
                        # use raise for status to catch errors
                        response.raise_for_status()
                        data = response.json()

                        if 'errors' in data:
                            context.log.info(f"Status Code: {response.status_code}")
                            # Extract rate limit information from headers
                            context.log.info(" \n resource usage tracking:")
                            rate_limit_info = {
                                'remaining': response.headers.get('x-ratelimit-remaining'),
                                'used': response.headers.get('x-ratelimit-used'),
                                'reset': response.headers.get('x-ratelimit-reset'),
                                'retry_after': response.headers.get('retry-after')
                            }
                            context.log.info(f"Rate Limit Info: {rate_limit_info}\n")

                            for error in data['errors']:
                                if error['type'] == 'RATE_LIMITED':
                                    reset_at = response.headers.get('X-RateLimit-Reset')
                                    if reset_at:
                                        delay = int(reset_at) - int(time.time()) + 1
                                        delay = max(1, delay)
                                        delay = min(delay, max_delay)
                                        context.log.warning(f"Rate limited.  Waiting for {delay} seconds...")
                                        time.sleep(delay)
                                        continue  # Retry the entire batch
                                else:
                                    context.log.warning(f"GraphQL Error: {error}") #Print all the errors.

                        # write the url and description to the database
                        if 'data' in data:
                            for j, repo_url in enumerate(batch):
                                if repo_url in processed_in_batch:  # CRUCIAL CHECK
                                    continue  # Skip if already processed

                                repo_data = data['data'].get(f'repo{j}')
                                if repo_data:
                                    results[repo_url] = repo_data['description']
                                    processed_in_batch.add(repo_url)  # Mark as processed
                                else:
                                    context.log.warning(f"repo_data is empty for repo: {repo_url}\n")
                                    # don't return here, return errors at end of batch
                        break

                    except requests.exceptions.RequestException as e:
                        context.log.warning(f"there was a request exception on attempt: {attempt}\n")
                        context.log.warning(f"procesing batch: {batch}\n")
                        context.log.warning(f"Status Code: {response.status_code}")

                        # Extract rate limit information from headers
                        context.log.warning(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        context.log.warning(f"Rate Limit Info: {rate_limit_info}\n")

                        context.log.warning(f"the error is: {e}\n")
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached or unrecoverable error for batch. Giving up.")
                            # don't return here, return errors at end of batch
                            break

                        # rate limit handling
                        if isinstance(e, requests.exceptions.HTTPError):
                            if e.response.status_code in (502, 504):
                                count_502_errors += 1
                                context.log.warning(f"This process has generated {count_502_errors} 502/504 errors in total.")
                                delay = 1
                                context.log.warning(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                            elif e.response.status_code in (403, 429):
                                count_403_errors += 1
                                context.log.warning(f"This process has generated {count_403_errors} 403/429 errors in total.")
                                retry_after = e.response.headers.get('Retry-After')
                                if retry_after:
                                    delay = int(retry_after)
                                    context.log.warning(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue
                                else:
                                    delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                    context.log.warning(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                    time.sleep(delay)
                                    continue
                        else:
                            delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                            context.log.warning(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)

                    except KeyError as e:
                        context.log.warning(f"KeyError: {e}. Response: {data}")
                        # Don't append here; handle errors at the end
                        break
                    except Exception as e:
                        context.log.warning(f"An unexpected error occurred: {e}")
                        # Don't append here; handle errors at the end
                        break

                # Handle any repos that failed *all* retries (or were invalid URLs)
                for repo_url in batch:
                    if repo_url not in processed_in_batch:
                        results[repo_url] = None
                        context.log.warning(f"adding repo to results after max retries, or was invalid url: {repo_url}")
                        processed_in_batch.add(repo_url)

                # calculate the time it takes to process the batch
                end_time = time.time()
                batch_time = end_time - start_time
                cpu_time_used += time_since_start
                real_time_used += batch_time
                batch_time_history.append(batch_time)
                if batch_time_history and len(batch_time_history) > 10:
                    context.log.info(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
                context.log.info(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
                context.log.info(f"time taken to process batch {i}: {batch_time:.2f} seconds")
                context.log.info(f"Total CPU time used: {cpu_time_used:.2f} seconds")
                context.log.info(f"Total real time used: {real_time_used:.2f} seconds")

            return results, {
                'count_403_errors': count_403_errors,
                'count_502_errors': count_502_errors
            }

        # Execute the query
        with cloud_sql_engine.connect() as conn:

            # query the latest_distinct_project_repos table to get the distinct repo list
            result = conn.execute(
                text(f"""select repo, repo_source from {clean_schema}.latest_active_distinct_project_repos where is_active = true""")
                    )
            repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Filter for GitHub URLs
        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

        # get github pat
        gh_pat = github_api.get_client(config.key_name)

        results = get_github_repo_description(github_urls, gh_pat)

        github_results = results[0]
        count_http_errors_github_api = results[1]

        # write results to pandas dataframe
        results_df = pd.DataFrame(github_results.items(), columns=['repo', 'description'])

        # now get non-github repos urls
        non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

        # if non_github_urls is not empty, get repo description
        if not non_github_results_df.empty:
            context.log.info("found non-github repos. Getting repo description...")
            # apply distinct_repo_df['repo'] to get repo description
            non_github_results_df['description'] = non_github_results_df.apply(
                lambda row: get_non_github_repo_description(row['repo'], row['repo_source']), axis=1
            )

            # drop the repo_source column
            non_github_results_df = non_github_results_df.drop(columns=['repo_source'])

            # append non_github_urls to results_df
            results_df = pd.concat([results_df, non_github_results_df])

        # add unix datetime column
        results_df['data_timestamp'] = pd.Timestamp.now()

        # write results to database
        results_df.to_sql('latest_project_repos_description', cloud_sql_engine, if_exists='replace', index=False, schema=raw_schema)

        with cloud_sql_engine.connect() as conn:
            # capture asset metadata
            preview_query = text(f"select count(*) from {raw_schema}.latest_project_repos_description")
            result = conn.execute(preview_query)
            # Fetch all rows into a list of tuples
            row_count = result.fetchone()[0]

            preview_query = text(f"select * from {raw_schema}.latest_project_repos_description limit 10")
            result = conn.execute(preview_query)
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                "count_403_errors": dg.MetadataValue.int(count_http_errors_github_api['count_403_errors']),
                "count_502_errors": dg.MetadataValue.int(count_http_errors_github_api['count_502_errors'])
            }
        )

    return _project_repos_description_env_specific # Return the decorated function


# It's good practice to have constants defined at the top
MAX_README_LENGTH = 100000  # Maximum characters to store for a README
REPO_PROCESSING_BATCH_SIZE = 500 # Number of repos to process in each batch

def create_project_repos_readmes_asset(env_prefix: str):
    """
    Factory function to create the project_repos_readmes asset
    with an environment-specific key_prefix.
    This asset fetches README files from various repository sources,
    cleaning them of images/links and truncating if they exceed a defined maximum length.
    """
    @dg.asset(
        key_prefix=env_prefix,
        name="project_repos_readmes",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},
        description="""
        This asset fetches README files from all active repositories in batches,
        cleaning them of images/links and truncating if they exceed a defined maximum length.
        """
    )
    def _project_repos_readmes_env_specific(context: dg.OpExecutionContext, config: GithubAssetConfig) -> dg.MaterializeResult:
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        raw_schema = env_config["raw_schema"]
        clean_schema = env_config["clean_schema"]
        github_api = context.resources.github_api

        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        gh_pat = github_api.get_client(config.key_name)
        if not gh_pat:
            context.log.warning("GitHub Personal Access Token not found in environment variables.")

        readme_filenames_to_try = ["README.md", "readme.md", "README.rst", "README.txt"]

        # No changes needed in helper functions
        def clean_readme_text(content: str) -> str:
            if not content:
                return ""
            content = content.replace('\x00', '')
            content = re.sub(r'!\[.*?\]\(.*?\)', '', content)
            content = re.sub(r'<img[^>]*>', '', content, flags=re.IGNORECASE)
            content = re.sub(r'\[([^\]]+)\]\(.*?\)', r'\1', content)
            content = re.sub(r'<a[^>]*>(.*?)<\/a>', r'\1', content, flags=re.IGNORECASE | re.DOTALL)
            return content

        def process_readme_content(content: str | None) -> tuple[str | None, bool]:
            if content is None:
                return None, False
            cleaned_content = clean_readme_text(content)
            if len(cleaned_content) > MAX_README_LENGTH:
                context.log.debug(f"Cleaned README content length {len(cleaned_content)} exceeds max {MAX_README_LENGTH}. Truncating.")
                return cleaned_content[:MAX_README_LENGTH], True
            return cleaned_content, False

        def fetch_readme_content_via_get(url: str) -> str | None:
            try:
                time.sleep(0.2)
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                context.log.warning(f"Failed to fetch {url}: {e}")
                return None

        def get_non_github_repo_readme(repo_url: str, repo_source: str) -> tuple[str | None, bool]:
            context.log.info(f"Processing non-GitHub repo: {repo_url} (Source: {repo_source})")
            time.sleep(0.5)
            for filename in readme_filenames_to_try:
                raw_content = None
                if repo_source == "bitbucket":
                    try:
                        parts = repo_url.rstrip('/').split('/')
                        owner, repo_slug = parts[-2], parts[-1].replace(".git", "")
                        for branch in ["master", "main", "develop"]:
                            api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}/src/{branch}/{filename}"
                            if raw_content := fetch_readme_content_via_get(api_url):
                                context.log.debug(f"Found {filename} for Bitbucket repo {repo_url} on branch {branch}")
                                return process_readme_content(raw_content)
                    except (IndexError, Exception) as e:
                        context.log.warning(f"Error processing Bitbucket URL {repo_url}: {e}")
                        continue
                elif repo_source == "gitlab":
                    try:
                        project_path = "/".join(repo_url.rstrip('/').split('/')[3:]).replace(".git", "")
                        project_path_encoded = requests.utils.quote(project_path, safe='')
                        for branch in ["master", "main", "develop"]:
                            api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}/repository/files/{requests.utils.quote(filename, safe='')}/raw?ref={branch}"
                            if raw_content := fetch_readme_content_via_get(api_url):
                                context.log.debug(f"Found {filename} for GitLab repo {repo_url} on branch {branch}")
                                return process_readme_content(raw_content)
                    except (IndexError, Exception) as e:
                        context.log.warning(f"Error processing GitLab URL {repo_url}: {e}")
                        continue
            context.log.info(f"No README found for {repo_source} repo: {repo_url}")
            return None, False

        def get_github_repo_readme(repo_urls: list[str], gh_pat_token: str | None) -> tuple[dict[str, tuple[str | None, bool]], dict]:
            if not repo_urls:
                return {}, {'count_403_errors': 0, 'count_502_errors': 0}
            if not gh_pat_token:
                context.log.error("GitHub PAT is missing. Cannot fetch GitHub READMEs.")
                return {url: (None, False) for url in repo_urls}, {'count_403_errors': 0, 'count_502_errors': 0}

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat_token}"}
            results: dict[str, tuple[str | None, bool]] = {}
            batch_size = 55
            error_counts = {'count_403_errors': 0, 'count_502_errors': 0}
            context.log.info(f"Starting GitHub README fetch for {len(repo_urls)} URLs.")
            for i in range(0, len(repo_urls), batch_size):
                batch_start_time = time.time()
                batch = repo_urls[i:i + batch_size]
                processed_in_batch = set()
                variables = {}
                final_query_parts = []
                repo_url_to_query_idx_map = {}
                current_query_idx = 0
                for repo_url_original in batch:
                    try:
                        parts = repo_url_original.rstrip('/').split('/')
                        owner, name = parts[-2], parts[-1].replace(".git", "")
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format, skipping: {repo_url_original}")
                        results[repo_url_original] = (None, False)
                        processed_in_batch.add(repo_url_original)
                        continue
                    var_owner, var_name = f"owner{current_query_idx}", f"name{current_query_idx}"
                    variables[var_owner], variables[var_name] = owner, name
                    repo_url_to_query_idx_map[repo_url_original] = current_query_idx
                    final_query_parts.append(f"""
                    repo{current_query_idx}: repository(owner: ${var_owner}, name: ${var_name}) {{
                        readmeMD: object(expression: "HEAD:README.md") {{ ... on Blob {{ text }} }}
                        readmemd: object(expression: "HEAD:readme.md") {{ ... on Blob {{ text }} }}
                        readmeRST: object(expression: "HEAD:README.rst") {{ ... on Blob {{ text }} }}
                        readmeTXT: object(expression: "HEAD:README.txt") {{ ... on Blob {{ text }} }}
                    }}""")
                    current_query_idx += 1
                if not final_query_parts: continue
                query_variable_definitions = ", ".join([f"$owner{k}: String!, $name{k}: String!" for k in range(current_query_idx)])
                full_query = f"query ({query_variable_definitions}) {{\n" + "\n".join(final_query_parts) + "\n}"
                context.log.info(f"Executing GitHub GraphQL batch query for {len(final_query_parts)} repos.")
                max_retries, base_delay = 7, 2
                for attempt in range(max_retries):
                    try:
                        response = requests.post(api_url, json={'query': full_query, 'variables': variables}, headers=headers, timeout=30)
                        response.raise_for_status()
                        data = response.json()
                        if 'errors' in data and data['errors']:
                            is_rate_limited = any(error.get('type') == 'RATE_LIMITED' for error in data['errors'])
                            if is_rate_limited:
                                error_counts['count_403_errors'] += 1
                                delay = min(float(response.headers.get('Retry-After', base_delay * (2 ** attempt))), 60)
                                context.log.warning(f"Rate limited. Retrying in {delay:.2f}s...")
                                time.sleep(delay)
                                continue
                        if 'data' in data:
                            for repo_url, query_idx in repo_url_to_query_idx_map.items():
                                if repo_url in processed_in_batch: continue
                                repo_api_data = data['data'].get(f'repo{query_idx}')
                                content = None
                                if repo_api_data:
                                    for key in ['readmeMD', 'readmemd', 'readmeRST', 'readmeTXT']:
                                        if obj := repo_api_data.get(key):
                                            if text_content := obj.get('text'):
                                                content = text_content
                                                break
                                results[repo_url] = process_readme_content(content)
                                processed_in_batch.add(repo_url)
                            break
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code in (502, 504): error_counts['count_502_errors'] += 1
                        elif e.response.status_code in (403, 429): error_counts['count_403_errors'] += 1
                        context.log.warning(f"HTTPError on attempt {attempt+1}: {e}. Retrying...")
                        time.sleep(min(float(e.response.headers.get('Retry-After', base_delay * (2 ** attempt))), 60))
                    except requests.exceptions.RequestException as e:
                        context.log.error(f"RequestException for GitHub batch, stopping retries for this batch: {e}")
                        break
                for repo_url in batch:
                    if repo_url not in processed_in_batch:
                        results[repo_url] = (None, False)
                context.log.info(f"GitHub batch processed in {time.time() - batch_start_time:.2f}s.")
                time.sleep(1)
            return results, error_counts

        # Main asset logic starts here
        # first set a consistent timestamp for all rows
        data_timestamp = pd.Timestamp.now()

        with cloud_sql_engine.connect() as conn:
            query = f"SELECT repo, repo_source FROM {clean_schema}.latest_active_distinct_project_repos_with_code"
            repo_df = pd.read_sql(query, conn)

        if repo_df.empty:
            context.log.info("No active repositories found to process.")
            return dg.MaterializeResult(metadata={"row_count": 0, "preview": "No active repositories found."})

        # Totals to aggregate across all batches
        total_records_processed = 0
        total_github_errors = {'count_403_errors': 0, 'count_502_errors': 0}
        
        target_table_name = "latest_project_repos_readmes"
        dtype_mapping = {
            'repo': sqlalchemy.types.Text,
            'readme_content': sqlalchemy.types.Text,
            'is_truncated': sqlalchemy.types.Boolean,
            'data_timestamp': sqlalchemy.types.TIMESTAMP(timezone=False)
        }

        num_batches = (len(repo_df) + REPO_PROCESSING_BATCH_SIZE - 1) // REPO_PROCESSING_BATCH_SIZE

        for i in range(0, len(repo_df), REPO_PROCESSING_BATCH_SIZE):
            batch_num = (i // REPO_PROCESSING_BATCH_SIZE) + 1
            context.log.info(f"--- Processing Batch {batch_num}/{num_batches} ---")
            
            batch_df = repo_df.iloc[i:i + REPO_PROCESSING_BATCH_SIZE]
            batch_results_list = []

            # Process GitHub repos in the current batch
            github_urls = batch_df[batch_df['repo_source'] == 'github']['repo'].tolist()
            if github_urls:
                github_readmes_data, github_errors = get_github_repo_readme(github_urls, gh_pat)
                total_github_errors['count_403_errors'] += github_errors.get('count_403_errors', 0)
                total_github_errors['count_502_errors'] += github_errors.get('count_502_errors', 0)
                for repo_url, (content, is_truncated) in github_readmes_data.items():
                    batch_results_list.append({'repo': repo_url, 'readme_content': content, 'is_truncated': is_truncated})

            # Process non-GitHub repos in the current batch
            non_github_df = batch_df[batch_df['repo_source'] != 'github']
            for _, row in non_github_df.iterrows():
                content, is_truncated = get_non_github_repo_readme(row['repo'], row['repo_source'])
                batch_results_list.append({'repo': row['repo'], 'readme_content': content, 'is_truncated': is_truncated})

            if not batch_results_list:
                context.log.warning(f"Batch {batch_num} resulted in no data.")
                continue
            
            # Create a DataFrame for the current batch
            batch_results_df = pd.DataFrame(batch_results_list)
            batch_results_df['data_timestamp'] = data_timestamp
            
            # Determine write method: 'replace' for first batch, 'append' for others
            write_method = 'replace' if i == 0 else 'append'

            try:
                batch_results_df.to_sql(
                    target_table_name,
                    cloud_sql_engine,
                    if_exists=write_method,
                    index=False,
                    schema=raw_schema,
                    dtype=dtype_mapping
                )
                context.log.info(f"Successfully wrote {len(batch_results_df)} records from batch {batch_num}.")
                total_records_processed += len(batch_results_df)
            except Exception as e:
                context.log.error(f"Error writing batch {batch_num} to database: {e}")
                # Optionally, you could add logic here to retry or handle the failed batch
        
        context.log.info(f"Finished processing all batches. Total records written: {total_records_processed}")

        # Final metadata generation
        row_count, preview_df_md = 0, "Table is empty or could not be queried."
        try:
            with cloud_sql_engine.connect() as conn:
                count_query = text(f"SELECT COUNT(*) FROM {raw_schema}.{target_table_name}")
                row_count = conn.execute(count_query).scalar_one()
                if row_count > 0:
                    preview_query = text(f"""
                        SELECT repo, LENGTH(readme_content) as readme_length, is_truncated, data_timestamp 
                        FROM {raw_schema}.{target_table_name} LIMIT 10
                    """)
                    preview_df = pd.read_sql(preview_query, conn)
                    preview_df_md = preview_df.to_markdown(index=False)
        except Exception as e:
            context.log.error(f"Failed to generate metadata from database: {e}")

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(preview_df_md),
                "github_api_403_errors": dg.MetadataValue.int(total_github_errors['count_403_errors']),
                "github_api_502_errors": dg.MetadataValue.int(total_github_errors['count_502_errors']),
                "max_readme_length": dg.MetadataValue.int(MAX_README_LENGTH),
                "repos_processed_in_batches_of": dg.MetadataValue.int(REPO_PROCESSING_BATCH_SIZE)
            }
        )
    return _project_repos_readmes_env_specific

# Extensible configuration for package manager files
PACKAGE_FILES_TO_TRY = {
    "npm": ["package.json"],
    "pypi": ["pyproject.toml", "setup.py", "setup.cfg"],
    "maven": ["pom.xml"],
    "gradle": ["build.gradle", "build.gradle.kts"],
    "gomod": ["go.mod"],
    "cargo": ["Cargo.toml"],
    "composer": ["composer.json"],
    "rubygems": ["Gemfile", ".gemspec"],
}
def create_project_repos_package_files_asset(env_prefix: str):
    """
    Factory function to create the project_repos_package_files asset.
    This asset finds and fetches package manager configuration files for all active repos.
    """
    @dg.asset(
        key_prefix=env_prefix,
        name="project_repos_package_files",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},
        description="""
        This asset searches for various package manager files (package.json, pyproject.toml, etc.)
        across all active repositories and stores their content.
        """
    )
    def _project_repos_package_files_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        raw_schema = env_config["raw_schema"]
        clean_schema = env_config["clean_schema"]

        # Access resources from the context object
        github_api = context.resources.github_api

        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        gh_pat = github_api.get_client(config.key_name)
        if not gh_pat:
            context.log.warning("GitHub Personal Access Token not found.")

        def fetch_content_via_get(url: str) -> str | None:
            """Helper to fetch content from a direct URL."""
            try:
                # A smaller delay is fine here as it's part of a larger loop
                time.sleep(0.1)
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                # Clean NUL characters that can break text processing
                return response.text.replace('\x00', '')
            except requests.exceptions.RequestException:
                # This is expected if a file doesn't exist, so no warning needed
                return None

        def get_non_github_package_files(repo_url: str, repo_source: str) -> list[dict]:
            """
            Fetches package manager files for non-GitHub repositories.
            Returns a list of found files, each as a dictionary.
            """
            context.log.debug(f"Processing non-GitHub repo: {repo_url} (Source: {repo_source})")
            found_files = []
            time.sleep(0.2) # Small delay between processing each non-github repo

            for manager, filenames in PACKAGE_FILES_TO_TRY.items():
                for filename in filenames:
                    raw_content = None
                    if repo_source == "bitbucket":
                        try:
                            parts = repo_url.rstrip('/').split('/')
                            owner, repo_slug = parts[-2], parts[-1].replace(".git", "")
                            for branch in ["main", "master"]:
                                api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}/src/{branch}/{filename}"
                                raw_content = fetch_content_via_get(api_url)
                                if raw_content: break
                        except (IndexError, Exception): continue
                    elif repo_source == "gitlab":
                        try:
                            project_path = "/".join(repo_url.rstrip('/').split('/')[3:]).replace(".git", "")
                            project_path_encoded = requests.utils.quote(project_path, safe='')
                            for branch in ["main", "master"]:
                                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}/repository/files/{requests.utils.quote(filename, safe='')}/raw?ref={branch}"
                                raw_content = fetch_content_via_get(api_url)
                                if raw_content: break
                        except (IndexError, Exception): continue
                    
                    if raw_content:
                        context.log.info(f"Found '{filename}' for {repo_source} repo: {repo_url}")
                        found_files.append({
                            "package_manager": manager,
                            "file_name": filename,
                            "file_content": raw_content
                        })
            return found_files


        def get_github_package_files(repo_urls: list[str], gh_pat_token: str | None) -> tuple[list[dict], dict]:
            """
            Queries package manager files for GitHub repositories using the GraphQL API.
            Returns a list of found files and a dictionary with error counts.
            """
            if not repo_urls: return [], {}
            if not gh_pat_token:
                context.log.error("GitHub PAT is missing. Cannot fetch GitHub package files.")
                return [], {}

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat_token}"}
            all_found_files = []
            batch_size = 40  # Smaller batch size due to potentially large query size
            errors = {'count_403_errors': 0, 'count_502_errors': 0}

            # Create a flat list of (alias, manager, filename, expression) for the query
            query_objects = []
            for manager, filenames in PACKAGE_FILES_TO_TRY.items():
                for filename in filenames:
                    alias = re.sub(r'[^a-zA-Z0-9_]', '_', filename)
                    query_objects.append((alias, manager, filename, f'HEAD:{filename}'))

            context.log.info(f"Starting GitHub package file fetch for {len(repo_urls)} URLs.")
            
            for i in range(0, len(repo_urls), batch_size):
                batch = repo_urls[i:i + batch_size]
                context.log.debug(f"Processing batch {i//batch_size + 1} of {len(repo_urls)//batch_size + 1}...")
                # every 1000 batches, print the progress
                if i % 1000 == 0:
                    context.log.info(f"Processing batch {i//batch_size + 1} of {len(repo_urls)//batch_size + 1}...")
                variables, query_parts = {}, []
                repo_url_map = {} # Maps repo_url to its query index (e.g., repo0, repo1)

                for idx, repo_url in enumerate(batch):
                    try:
                        owner, name = repo_url.rstrip('/').split('/')[-2:]
                        name = name.replace(".git", "")
                        var_owner, var_name = f"owner{idx}", f"name{idx}"
                        variables[var_owner], variables[var_name] = owner, name
                        repo_url_map[repo_url] = idx

                        # Build the dynamic query part for this repo
                        object_queries = "\n".join([f'{alias}: object(expression: "{expr}") {{ ... on Blob {{ text }} }}' for alias, _, _, expr in query_objects])
                        query_parts.append(f'repo{idx}: repository(owner: ${var_owner}, name: ${var_name}) {{\n{object_queries}\n}}')
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format, skipping: {repo_url}")
                        continue
                
                if not query_parts: 
                    context.log.warning(f"No query parts found for batch {i//batch_size + 1} of {len(repo_urls)//batch_size + 1}. Skipping.")
                    continue

                var_defs = ", ".join([f"$owner{k}: String!, $name{k}: String!" for k in range(len(batch))])
                full_query = f"query ({var_defs}) {{\n" + "\n".join(query_parts) + "\n}"
                
                # Simplified retry logic based on the template
                max_retries, base_delay = 3, 2
                for attempt in range(max_retries):
                    try:
                        response = requests.post(api_url, json={'query': full_query, 'variables': variables}, headers=headers, timeout=45)
                        response.raise_for_status()
                        data = response.json()

                        if 'data' in data and data['data']:
                            for repo_url, idx in repo_url_map.items():
                                repo_api_data = data['data'].get(f'repo{idx}')
                                if repo_api_data:
                                    for alias, manager, filename, _ in query_objects:
                                        if repo_api_data.get(alias) and repo_api_data[alias].get('text') is not None:
                                            content = repo_api_data[alias]['text'].replace('\x00', '')
                                            all_found_files.append({
                                                "repo": repo_url,
                                                "package_manager": manager,
                                                "file_name": filename,
                                                "file_content": content
                                            })
                                            context.log.debug(f"Found '{filename}' for GitHub repo: {repo_url}")
                                else:
                                    context.log.warning(f"Could not find GitHub repo {repo_url} in batch response (repo url may not be active or not found). Skipping.")
                        break # Success
                    except requests.exceptions.RequestException as e:
                        context.log.warning(f"GraphQL request failed on attempt {attempt+1}: {e}")
                        time.sleep(base_delay * (2 ** attempt))

            return all_found_files, errors

        # Main asset logic
        with cloud_sql_engine.connect() as conn:
            repo_df = pd.DataFrame(conn.execute(text(f"SELECT repo, repo_source FROM {clean_schema}.latest_active_distinct_project_repos_with_code")).fetchall())

        if repo_df.empty:
            context.log.info("No active repositories found to process.")
            return dg.MaterializeResult(metadata={"row_count": 0})

        # Process GitHub repos
        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()
        final_results_list, github_errors = get_github_package_files(github_urls, gh_pat)
        
        # Process non-GitHub repos
        non_github_df = repo_df[repo_df['repo_source'] != 'github']
        if not non_github_df.empty:
            context.log.info(f"Processing {len(non_github_df)} non-GitHub repositories...")
            for _, row in non_github_df.iterrows():
                repo_url, repo_source = row['repo'], row['repo_source']
                found_files = get_non_github_package_files(repo_url, repo_source)
                for file_info in found_files:
                    file_info['repo'] = repo_url
                    final_results_list.append(file_info)

        if not final_results_list:
            context.log.warning("No package manager files were found for any repository.")
            return dg.MaterializeResult(metadata={"row_count": 0})

        results_df = pd.DataFrame(final_results_list)
        results_df['data_timestamp'] = pd.Timestamp.now(tz='UTC')

        target_table_name = "latest_project_repos_package_files"
        try:
            dtype_mapping = {
                'repo': sqlalchemy.types.Text,
                'package_manager': sqlalchemy.types.Text,
                'file_name': sqlalchemy.types.Text,
                'file_content': sqlalchemy.types.Text,
                'data_timestamp': sqlalchemy.types.TIMESTAMP(timezone=False)
            }
            results_df.to_sql(target_table_name, cloud_sql_engine, if_exists='replace', index=False, schema=raw_schema, dtype=dtype_mapping)
            context.log.info(f"Successfully wrote {len(results_df)} package file entries to {raw_schema}.{target_table_name}")
        except Exception as e:
            context.log.error(f"Error writing package files to database: {e}")

        # Metadata for Dagster UI
        row_count = len(results_df)
        preview_df = results_df[['repo', 'package_manager', 'file_name']].head(10)

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "github_api_403_errors": dg.MetadataValue.int(github_errors.get('count_403_errors',0)),
                "github_api_502_errors": dg.MetadataValue.int(github_errors.get('count_502_errors',0)),
                "package_managers_searched": dg.MetadataValue.text(", ".join(PACKAGE_FILES_TO_TRY.keys()))
            }
        )
    return _project_repos_package_files_env_specific


# Define the dictionary of contract application developmentframework config files to search for
CONFIG_FILES_TO_TRY = {
    "hardhat": ["hardhat.config.ts", "hardhat.config.js", "hardhat.config.cjs", "hardhat.config.mjs"],
    "foundry": ["foundry.toml"],
    "truffle": ["truffle-config.js", "truffle-config.json", "truffle-config.yaml", "truffle-config.yml"],
    "brownie": ["brownie-config.yaml", "brownie-config.yml"],
    "anchor": ["Anchor.toml"],
}

def create_project_repos_app_dev_framework_files_asset(env_prefix: str):
    """
    Factory function to create the project_repos_framework_files asset.
    This asset finds and fetches smart contract framework configuration files for all active repos.
    """
    @dg.asset(
        key_prefix=env_prefix,
        name="project_repos_app_dev_framework_files",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},
        description="""
        This asset searches for various smart contract framework config files (hardhat.config.js, foundry.toml, etc.)
        across all active repositories and stores their content.
        """
    )
    def _project_repos_app_dev_framework_files_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        raw_schema = env_config["raw_schema"]
        clean_schema = env_config["clean_schema"]

        # Access resources from the context object
        github_api = context.resources.github_api

        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        gh_pat = github_api.get_client(config.key_name)
        if not gh_pat:
            context.log.warning("GitHub Personal Access Token not found.")

        def fetch_content_via_get(url: str) -> str | None:
            """Helper to fetch content from a direct URL."""
            try:
                time.sleep(0.1)
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.text.replace('\x00', '')
            except requests.exceptions.RequestException:
                return None

        def get_non_github_framework_files(repo_url: str, repo_source: str) -> list[dict]:
            """
            Fetches framework config files for non-GitHub repositories.
            Returns a list of found files, each as a dictionary.
            """
            context.log.debug(f"Processing non-GitHub repo: {repo_url} (Source: {repo_source})")
            found_files = []
            time.sleep(0.2)

            # Use the CONFIG_FILES_TO_TRY dictionary
            for framework, filenames in CONFIG_FILES_TO_TRY.items():
                for filename in filenames:
                    raw_content = None
                    if repo_source == "bitbucket":
                        try:
                            parts = repo_url.rstrip('/').split('/')
                            owner, repo_slug = parts[-2], parts[-1].replace(".git", "")
                            for branch in ["main", "master"]:
                                api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}/src/{branch}/{filename}"
                                raw_content = fetch_content_via_get(api_url)
                                if raw_content: break
                        except (IndexError, Exception): continue
                    elif repo_source == "gitlab":
                        try:
                            project_path = "/".join(repo_url.rstrip('/').split('/')[3:]).replace(".git", "")
                            project_path_encoded = requests.utils.quote(project_path, safe='')
                            for branch in ["main", "master"]:
                                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}/repository/files/{requests.utils.quote(filename, safe='')}/raw?ref={branch}"
                                raw_content = fetch_content_via_get(api_url)
                                if raw_content: break
                        except (IndexError, Exception): continue
                    
                    if raw_content:
                        context.log.info(f"Found '{filename}' for {repo_source} repo: {repo_url}")
                        found_files.append({
                            "framework_name": framework, 
                            "file_name": filename,
                            "file_content": raw_content
                        })
            return found_files


        def get_github_framework_files(repo_urls: list[str], gh_pat_token: str | None) -> tuple[list[dict], dict]:
            """
            Queries framework config files for GitHub repositories using the GraphQL API.
            Returns a list of found files and a dictionary with error counts.
            """
            if not repo_urls: return [], {}
            if not gh_pat_token:
                context.log.error("GitHub PAT is missing. Cannot fetch GitHub framework files.")
                return [], {}

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat_token}"}
            all_found_files = []
            batch_size = 40
            errors = {'count_403_errors': 0, 'count_502_errors': 0}

            query_objects = []
            # Use the CONFIG_FILES_TO_TRY dictionary
            for framework, filenames in CONFIG_FILES_TO_TRY.items():
                for filename in filenames:
                    alias = re.sub(r'[^a-zA-Z0-9_]', '_', filename)
                    query_objects.append((alias, framework, filename, f'HEAD:{filename}'))

            context.log.info(f"Starting GitHub framework file fetch for {len(repo_urls)} URLs.")
            
            for i in range(0, len(repo_urls), batch_size):
                batch = repo_urls[i:i + batch_size]
                if i % 1000 == 0:
                    context.log.info(f"Processing batch {i//batch_size + 1} of {len(repo_urls)//batch_size + 1}...")

                variables, query_parts, repo_url_map = {}, [], {}
                for idx, repo_url in enumerate(batch):
                    try:
                        owner, name = repo_url.rstrip('/').split('/')[-2:]
                        name = name.replace(".git", "")
                        variables[f"owner{idx}"], variables[f"name{idx}"] = owner, name
                        repo_url_map[repo_url] = idx
                        object_queries = "\n".join([f'{alias}: object(expression: "{expr}") {{ ... on Blob {{ text }} }}' for alias, _, _, expr in query_objects])
                        query_parts.append(f'repo{idx}: repository(owner: $owner{idx}, name: $name{idx}) {{\n{object_queries}\n}}')
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format, skipping: {repo_url}")
                        continue
                
                if not query_parts: continue
                var_defs = ", ".join([f"$owner{k}: String!, $name{k}: String!" for k in range(len(batch))])
                full_query = f"query ({var_defs}) {{\n" + "\n".join(query_parts) + "\n}"
                
                max_retries, base_delay = 7, 2
                for attempt in range(max_retries):
                    try:
                        response = requests.post(api_url, json={'query': full_query, 'variables': variables}, headers=headers, timeout=45)
                        response.raise_for_status()
                        data = response.json()
                        if 'data' in data and data['data']:
                            for repo_url, idx in repo_url_map.items():
                                repo_api_data = data['data'].get(f'repo{idx}')
                                if repo_api_data:
                                    for alias, framework, filename, _ in query_objects:
                                        if repo_api_data.get(alias) and repo_api_data[alias].get('text') is not None:
                                            content = repo_api_data[alias]['text'].replace('\x00', '')
                                            all_found_files.append({
                                                "repo": repo_url,
                                                "framework_name": framework, 
                                                "file_name": filename,
                                                "file_content": content
                                            })
                                            context.log.debug(f"Found '{filename}' for GitHub repo: {repo_url}")
                        break 
                    except requests.exceptions.RequestException as e:
                        context.log.warning(f"GraphQL request failed on attempt {attempt+1}: {e}")
                        time.sleep(base_delay * (2 ** attempt))

                # delay for 1 second to avoid rate limiting
                time.sleep(1)

            return all_found_files, errors

        # Main asset logic
        with cloud_sql_engine.connect() as conn:
            repo_df = pd.DataFrame(conn.execute(text(f"SELECT repo, repo_source FROM {clean_schema}.latest_active_distinct_project_repos_with_code")).fetchall())

        if repo_df.empty:
            context.log.info("No active repositories found to process.")
            return dg.MaterializeResult(metadata={"row_count": 0})

        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()
        final_results_list, github_errors = get_github_framework_files(github_urls, gh_pat)
        
        non_github_df = repo_df[repo_df['repo_source'] != 'github']
        if not non_github_df.empty:
            context.log.info(f"Processing {len(non_github_df)} non-GitHub repositories...")
            for _, row in non_github_df.iterrows():
                repo_url, repo_source = row['repo'], row['repo_source']
                found_files = get_non_github_framework_files(repo_url, repo_source)
                for file_info in found_files:
                    file_info['repo'] = repo_url
                    final_results_list.append(file_info)

        if not final_results_list:
            context.log.warning("No framework config files were found for any repository.")
            return dg.MaterializeResult(metadata={"row_count": 0})

        results_df = pd.DataFrame(final_results_list)
        results_df['data_timestamp'] = pd.Timestamp.now(tz='UTC')

        target_table_name = "latest_project_repos_framework_files" 
        try:
            dtype_mapping = {
                'repo': sqlalchemy.types.Text,
                'framework_name': sqlalchemy.types.Text, 
                'file_name': sqlalchemy.types.Text,
                'file_content': sqlalchemy.types.Text,
                'data_timestamp': sqlalchemy.types.TIMESTAMP(timezone=False)
            }
            results_df.to_sql(target_table_name, cloud_sql_engine, if_exists='replace', index=False, schema=raw_schema, dtype=dtype_mapping)
            context.log.info(f"Successfully wrote {len(results_df)} framework file entries to {raw_schema}.{target_table_name}")
        except Exception as e:
            context.log.error(f"Error writing framework files to database: {e}")
            raise

        row_count = len(results_df)
        preview_df = results_df[['repo', 'framework_name', 'file_name']].head(10)

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "github_api_403_errors": dg.MetadataValue.int(github_errors.get('count_403_errors',0)),
                "github_api_502_errors": dg.MetadataValue.int(github_errors.get('count_502_errors',0)),
                "frameworks_searched": dg.MetadataValue.text(", ".join(CONFIG_FILES_TO_TRY.keys()))
            }
        )
    return _project_repos_app_dev_framework_files_env_specific

# Define the dictionary of front-end development framework config files to search for
FRONTEND_CONFIG_FILES_TO_TRY = {
    # "webpack": ["webpack.common.js", "webpack.common.ts"],
    "vite": ["vite.config.ts", "vite.config.js"],
    "tailwind": ["tailwind.config.js", "tailwind.config.ts"],
    "vue": ["vue.config.js"],
    "next": ["next.config.js", "next.config.ts"],
    "nuxt": ["nuxt.config.ts", "nuxt.config.js"],
    "angular": ["angular.json"],
}
def create_project_repos_frontend_framework_files_asset(env_prefix: str):
    """
    Factory function to create the project_repos_frontend_framework_files asset.
    This asset finds and fetches front-end framework configuration files for all active repos.
    """
    @dg.asset(
        key_prefix=env_prefix,
        name="project_repos_frontend_framework_files",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},
        description="""
        This asset searches for various front-end framework config files (webpack.config.js, vite.config.ts, etc.)
        across all active repositories and stores their content.
        """
    )
    def _project_repos_frontend_framework_files_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        raw_schema = env_config["raw_schema"]
        clean_schema = env_config["clean_schema"]

        # Access resources from the context object
        github_api = context.resources.github_api

        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        gh_pat = github_api.get_client(config.key_name)
        if not gh_pat:
            context.log.warning("GitHub Personal Access Token not found.")

        def fetch_content_via_get(url: str) -> str | None:
            """Helper to fetch content from a direct URL."""
            try:
                time.sleep(0.1)
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.text.replace('\x00', '')
            except requests.exceptions.RequestException:
                return None

        def get_non_github_framework_files(repo_url: str, repo_source: str) -> list[dict]:
            """
            Fetches framework config files for non-GitHub repositories.
            Returns a list of found files, each as a dictionary.
            """
            context.log.debug(f"Processing non-GitHub repo: {repo_url} (Source: {repo_source})")
            found_files = []
            time.sleep(0.2)

            # Use the FRONTEND_CONFIG_FILES_TO_TRY dictionary
            for framework, filenames in FRONTEND_CONFIG_FILES_TO_TRY.items():
                for filename in filenames:
                    raw_content = None
                    if repo_source == "bitbucket":
                        try:
                            parts = repo_url.rstrip('/').split('/')
                            owner, repo_slug = parts[-2], parts[-1].replace(".git", "")
                            for branch in ["main", "master"]:
                                api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}/src/{branch}/{filename}"
                                raw_content = fetch_content_via_get(api_url)
                                if raw_content: break
                        except (IndexError, Exception): continue
                    elif repo_source == "gitlab":
                        try:
                            project_path = "/".join(repo_url.rstrip('/').split('/')[3:]).replace(".git", "")
                            project_path_encoded = requests.utils.quote(project_path, safe='')
                            for branch in ["main", "master"]:
                                api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}/repository/files/{requests.utils.quote(filename, safe='')}/raw?ref={branch}"
                                raw_content = fetch_content_via_get(api_url)
                                if raw_content: break
                        except (IndexError, Exception): continue
                    
                    if raw_content:
                        context.log.info(f"Found '{filename}' for {repo_source} repo: {repo_url}")
                        found_files.append({
                            "file_name": filename,
                            "file_content": raw_content
                        })
            return found_files


        def get_github_framework_files(repo_urls: list[str], gh_pat_token: str | None) -> tuple[list[dict], dict]:
            """
            Queries framework config files for GitHub repositories using the GraphQL API.
            Returns a list of found files and a dictionary with error counts.
            """
            if not repo_urls: return [], {}
            if not gh_pat_token:
                context.log.error("GitHub PAT is missing. Cannot fetch GitHub framework files.")
                return [], {}

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat_token}"}
            all_found_files = []
            batch_size = 35
            errors = {'count_403_errors': 0, 'count_502_errors': 0}

            query_objects = []
            # Use the new FRONTEND_CONFIG_FILES_TO_TRY dictionary
            for framework, filenames in FRONTEND_CONFIG_FILES_TO_TRY.items():
                for filename in filenames:
                    alias = re.sub(r'[^a-zA-Z0-9_]', '_', filename)
                    query_objects.append((alias, framework, filename, f'HEAD:{filename}'))

            context.log.info(f"Starting GitHub front-end framework file fetch for {len(repo_urls)} URLs.")
            
            for i in range(0, len(repo_urls), batch_size):
                batch = repo_urls[i:i + batch_size]
                if i % 1000 == 0:
                    context.log.info(f"Processing batch {i//batch_size + 1} of {len(repo_urls)//batch_size + 1}...")

                variables, query_parts, repo_url_map = {}, [], {}
                for idx, repo_url in enumerate(batch):
                    try:
                        owner, name = repo_url.rstrip('/').split('/')[-2:]
                        name = name.replace(".git", "")
                        variables[f"owner{idx}"], variables[f"name{idx}"] = owner, name
                        repo_url_map[repo_url] = idx
                        object_queries = "\n".join([f'{alias}: object(expression: "{expr}") {{ ... on Blob {{ text }} }}' for alias, _, _, expr in query_objects])
                        query_parts.append(f'repo{idx}: repository(owner: $owner{idx}, name: $name{idx}) {{\n{object_queries}\n}}')
                    except IndexError:
                        context.log.warning(f"Invalid GitHub URL format, skipping: {repo_url}")
                        continue
                
                if not query_parts: 
                    continue
                var_defs = ", ".join([f"$owner{k}: String!, $name{k}: String!" for k in range(len(batch))])
                full_query = f"query ({var_defs}) {{\n" + "\n".join(query_parts) + "\n}"
                
                max_retries, base_delay = 7, 2
                for attempt in range(max_retries):
                    try:
                        response = requests.post(api_url, json={'query': full_query, 'variables': variables}, headers=headers, timeout=45)
                        response.raise_for_status()
                        data = response.json()
                        if 'data' in data and data['data']:
                            for repo_url, idx in repo_url_map.items():
                                repo_api_data = data['data'].get(f'repo{idx}')
                                if repo_api_data:
                                    for alias, framework, filename, _ in query_objects:
                                        if repo_api_data.get(alias) and repo_api_data[alias].get('text') is not None:
                                            content = repo_api_data[alias]['text'].replace('\x00', '')
                                            all_found_files.append({
                                                "repo": repo_url,
                                                "file_name": filename,
                                                "file_content": content
                                            })
                                            context.log.debug(f"Found '{filename}' for GitHub repo: {repo_url}")
                        break 
                    except requests.exceptions.RequestException as e:
                        context.log.warning(f"GraphQL request failed on attempt {attempt+1}: {e}")
                        time.sleep(base_delay * (2 ** attempt))
                
                time.sleep(1)

            return all_found_files, errors

        # Main asset logic
        with cloud_sql_engine.connect() as conn:
            repo_df = pd.DataFrame(conn.execute(text(f"SELECT repo, repo_source FROM {clean_schema}.latest_active_distinct_project_repos_with_code")).fetchall())

        if repo_df.empty:
            context.log.info("No active repositories found to process.")
            return dg.MaterializeResult(metadata={"row_count": 0})

        github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()
        final_results_list, github_errors = get_github_framework_files(github_urls, gh_pat)
        
        non_github_df = repo_df[repo_df['repo_source'] != 'github']
        if not non_github_df.empty:
            context.log.info(f"Processing {len(non_github_df)} non-GitHub repositories...")
            for _, row in non_github_df.iterrows():
                repo_url, repo_source = row['repo'], row['repo_source']
                found_files = get_non_github_framework_files(repo_url, repo_source)
                for file_info in found_files:
                    file_info['repo'] = repo_url
                    final_results_list.append(file_info)

        if not final_results_list:
            context.log.warning("No front-end framework config files were found for any repository.")
            return dg.MaterializeResult(metadata={"row_count": 0})

        results_df = pd.DataFrame(final_results_list)
        results_df['data_timestamp'] = pd.Timestamp.now(tz='UTC')

        # write to target table in raw schema
        target_table_name = "latest_project_repos_frontend_framework_files" 
        try:
            dtype_mapping = {
                'repo': sqlalchemy.types.Text,
                'file_name': sqlalchemy.types.Text,
                'file_content': sqlalchemy.types.Text,
                'data_timestamp': sqlalchemy.types.TIMESTAMP(timezone=False)
            }
            results_df.to_sql(target_table_name, cloud_sql_engine, if_exists='replace', index=False, schema=raw_schema, dtype=dtype_mapping)
            context.log.info(f"Successfully wrote {len(results_df)} front-end framework file entries to {raw_schema}.{target_table_name}")
        except Exception as e:
            context.log.error(f"Error writing front-end framework files to database: {e}")
            raise

        row_count = len(results_df)
        preview_df = results_df[['repo', 'file_name']].head(10)

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "github_api_403_errors": dg.MetadataValue.int(github_errors.get('count_403_errors',0)),
                "github_api_502_errors": dg.MetadataValue.int(github_errors.get('count_502_errors',0)),
                "frameworks_searched": dg.MetadataValue.text(", ".join(FRONTEND_CONFIG_FILES_TO_TRY.keys())) # Updated metadata
            }
        )
    return _project_repos_frontend_framework_files_env_specific

# Define config files and exclusion keywords at a broader scope
CONFIG_FILES = [".readthedocs.yaml", "docusaurus.config.js", "conf.py", ".gitbook.yaml"]
KEYWORDS_TO_EXCLUDE = ["readme", "license", "contributors", "contribution", "changelog", "upgrading", "upgrade", "history", "changes", "contributing"]
def get_documentation_files(context: dg.OpExecutionContext, gh_pat: str | None, session: requests.Session, repo_url: str, repo_source: str) -> list[dict]:
    """
    Fetches documentation files for a single repository based on its source.
    Returns a list of found files, each as a dictionary.
    """
    found_files = []
    headers = {}
    if repo_source == "github" and gh_pat:
        headers = {"Authorization": f"bearer {gh_pat}"}
    
    try:
        owner, repo_name = repo_url.rstrip('/').replace(".git", "").split('/')[-2:]
        default_branch = "main"
        
        if repo_source == "github":
            # Fetches default branch
            api_url = f"https://api.github.com/repos/{owner}/{repo_name}"
            response = session.get(api_url, headers=headers, timeout=15)
            default_branch = response.json().get("default_branch", "master") if response.ok else "master"

        # --- 1. Get the recursive file tree ---
        file_paths = []
        if repo_source == "github":
            tree_url = f"https://api.github.com/repos/{owner}/{repo_name}/git/trees/{default_branch}?recursive=1"
            response = session.get(tree_url, headers=headers, timeout=30)
            response.raise_for_status()
            tree_data = response.json().get("tree", [])
            file_paths = [item['path'] for item in tree_data if item['type'] == 'blob']
        
        elif repo_source == "gitlab":
            # GitLab logic remains the same
            project_path_encoded = requests.utils.quote(f"{owner}/{repo_name}", safe='')
            tree_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}/repository/tree?recursive=true&per_page=100" # Note: GitLab API is paginated, this may not be exhaustive for >100 files
            response = session.get(tree_url, timeout=30)
            response.raise_for_status()
            file_paths = [item['path'] for item in response.json() if item['type'] == 'blob']
        
        if not file_paths:
            return []

        # --- 2. STRATEGY: FIND DOCS ROOT & FILTER FILE LIST ---
        docs_root = None
        for path in file_paths:
            path_lower = path.lower()
            if os.path.basename(path_lower) in CONFIG_FILES or ".vitepress/config.js" in path_lower:
                docs_root = os.path.dirname(path)
                # Handle case where config is in root, dirname returns empty string
                if docs_root == ".": docs_root = ""
                context.log.info(f"Found config file '{path}'. Docs root is '{docs_root or './'}' for {repo_url}")
                break
        
        files_to_process = []
        if docs_root is not None:
            # If a root was found, only process files within that directory.
            # Add a trailing slash for accurate `startswith` matching, unless it's the repo root.
            root_prefix = f"{docs_root}/" if docs_root else ""
            files_to_process = [
                p for p in file_paths
                if (
                    p.startswith(root_prefix) and
                    p.lower().endswith((".md", ".rst")) and
                    # filename exclusion logic
                    not any(keyword in os.path.basename(p.lower()) for keyword in KEYWORDS_TO_EXCLUDE)
                )
            ]

        # --- 3. Fetch content for the targeted files ---
        if len(files_to_process) > 0:
            context.log.info(f"Found {len(files_to_process)} documentation files to fetch for {repo_url}. Fetching...")

            for path in files_to_process:
                content_url = ""
                if repo_source == "github":
                    content_url = f"https://raw.githubusercontent.com/{owner}/{repo_name}/{default_branch}/{path}"
                elif repo_source == "gitlab":
                    project_path_encoded = requests.utils.quote(f"{owner}/{repo_name}", safe='')
                    path_encoded = requests.utils.quote(path, safe='')
                    content_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}/repository/files/{path_encoded}/raw?ref={default_branch}"

                if content_url:
                    time.sleep(0.1) # Rate limiting
                    content_response = session.get(content_url, headers=headers, timeout=20)
                    if content_response.status_code == 200:
                        found_files.append({
                            "repo": repo_url,
                            "file_name": os.path.basename(path),
                            "file_content": content_response.text.replace('\x00', '')
                        })
            
            context.log.info(f"Successfully fetched {len(found_files)} documentation files for {repo_url}.")
        else:
            found_files = []
    except requests.exceptions.RequestException as e:
        context.log.warning(f"Failed to process repo {repo_url}: {e}")
    except Exception as e:
        context.log.error(f"An unexpected error occurred for repo {repo_url}: {e}")

    return found_files
def create_project_repos_documentation_files_asset(env_prefix: str):
    """
    Factory function to create the project_repos_documentation_files asset.
    This asset finds and fetches documentation files (.md, .rst) for all active repos.
    """
    @dg.asset(
        key_prefix=env_prefix,
        name="project_repos_documentation_files",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"},
        description="""
        This asset searches for documentation files (*.md, *.rst) across all active repositories,
        excluding README.md and LICENSE.md, and stores their content.
        """
    )
    def _project_repos_documentation_files_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        raw_schema = env_config["raw_schema"]
        clean_schema = env_config["clean_schema"]

        # Access resources from the context object
        github_api = context.resources.github_api

        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        gh_pat = github_api.get_client(config.key_name)
        if not gh_pat:
            context.log.warning("GitHub Personal Access Token not found.")

        # Create a session object with a retry policy
        session = requests.Session()
        retry_strategy = Retry(
            total=7,
            status_forcelist=[500, 502, 503, 504], # Retry on server-side errors
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        with cloud_sql_engine.connect() as conn:
            repo_df = pd.DataFrame(conn.execute(text(f"""
                SELECT 
                    r.repo, r.repo_source 
                FROM 
                    {clean_schema}.latest_active_distinct_project_repos_with_code r
                LEFT JOIN {clean_schema}.latest_project_repos_is_fork f
                ON r.repo = f.repo
                WHERE f.is_fork = false
            """)).fetchall())

        if repo_df.empty:
            context.log.info("No active repositories found to process.")
            return dg.MaterializeResult(metadata={"row_count": 0})

        final_results_list = []
        total_repos = len(repo_df)
        context.log.info(f"Starting documentation file fetch for {total_repos} repositories...")
        for index, row in repo_df.iterrows():
            # Add a delay between processing each repository to respect rate limits
            time.sleep(0.5)
            repo_url, repo_source = row['repo'], row['repo_source']
            # print every 100 repos
            if (index + 1) % 100 == 0:
                context.log.info(f"Processing repo {index + 1}/{total_repos}: {repo_url}")
            final_results_list.extend(get_documentation_files(context, gh_pat, session, repo_url, repo_source))

        if not final_results_list:
            context.log.warning("No documentation files were found for any repository.")
            return dg.MaterializeResult(metadata={"row_count": 0})

        results_df = pd.DataFrame(final_results_list)
        results_df['data_timestamp'] = pd.Timestamp.now(tz='UTC')

        target_table_name = "latest_project_repos_documentation_files" 
        try:
            dtype_mapping = {
                'repo': sqlalchemy.types.Text,
                'file_name': sqlalchemy.types.Text,
                'file_content': sqlalchemy.types.Text,
                'data_timestamp': sqlalchemy.types.TIMESTAMP(timezone=False)
            }
            results_df.to_sql(target_table_name, cloud_sql_engine, if_exists='replace', index=False, schema=raw_schema, dtype=dtype_mapping)
            context.log.info(f"Successfully wrote {len(results_df)} documentation file entries to {raw_schema}.{target_table_name}")
        except Exception as e:
            context.log.error(f"Error writing documentation files to database: {e}")
            raise

        row_count = len(results_df)
        preview_df = results_df[['repo', 'file_name']].head(10)

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "file_types_searched": dg.MetadataValue.text("*.md, *.rst")
            }
        )
    return _project_repos_documentation_files_env_specific

################################################### get repo contributors using github rest api ###################################################
# Define a constant for batch size
BATCH_SIZE = 50

# --- ETag Helper Functions
def _create_etag_table_if_not_exists(context: dg.OpExecutionContext, schema: str, table_name: str):
    """Checks for and creates the ETag tracking table if it doesn't exist."""
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    inspector = inspect(cloud_sql_engine)
    if not inspector.has_table(table_name, schema=schema):
        context.log.info(f"Table '{schema}.{table_name}' not found. Creating it...")
        with cloud_sql_engine.connect() as conn:
            conn.execute(text(f"""
                CREATE TABLE {schema}.{table_name} (
                    repo_url VARCHAR PRIMARY KEY,
                    etag VARCHAR,
                    data_timestamp TIMESTAMP WITHOUT TIME ZONE
                );
            """))
            conn.commit()
        context.log.info(f"Table '{schema}.{table_name}' created successfully.")

def _get_etags_from_db(context: dg.OpExecutionContext, schema: str, table_name: str, repo_urls: list) -> dict:
    """Fetches existing ETags for a list of repo URLs from the database."""
    if not repo_urls:
        return {}
    
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    query = text(f"SELECT repo_url, etag FROM {schema}.{table_name} WHERE repo_url = ANY(:repo_urls)")
    
    with cloud_sql_engine.connect() as conn:
        result = conn.execute(query, {"repo_urls": repo_urls})
        return {row[0]: row[1] for row in result}

def _update_etags_in_db(context: dg.OpExecutionContext, schema:str, table_name: str, etags_to_update: dict):
    """Upserts ETags into the database using a temporary table method."""
    if not etags_to_update:
        return
        
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    update_data = [
        {"repo_url": repo, "etag": etag, "data_timestamp": pd.Timestamp.now()}
        for repo, etag in etags_to_update.items()
    ]
    temp_table_name = f"temp_{table_name}_upsert"
    df = pd.DataFrame(update_data)

    with cloud_sql_engine.begin() as conn:
        df.to_sql(temp_table_name, conn, if_exists='replace', index=False)
        upsert_query = text(f"""
            INSERT INTO {schema}.{table_name} (repo_url, etag, data_timestamp)
            SELECT repo_url, etag, data_timestamp FROM {temp_table_name}
            ON CONFLICT (repo_url) DO UPDATE SET
                etag = EXCLUDED.etag,
                data_timestamp = EXCLUDED.data_timestamp;
        """)
        conn.execute(upsert_query)
        conn.execute(text(f"DROP TABLE {temp_table_name};"))
    context.log.info(f"Successfully upserted {len(etags_to_update)} ETags into '{schema}.{table_name}'.")


# define the asset that gets the contributors for a repo
def create_github_project_repos_contributors_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="github_project_repos_contributors",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={
            "github_api": "True",
            "contributor_lock": "True"
        },
    )
    def _github_project_repos_contributors_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 
        github_api = context.resources.github_api
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        gh_pat = github_api.get_client(config.key_name)

        etag_table_name = "github_project_repos_contributors_etags"
        target_table_name = "project_repos_contributors"
        # Create a unique, sanitized name for the staging table for this specific run
        safe_run_id = context.run_id.replace("-", "_")
        staging_table_name = f"staging_{target_table_name}_{safe_run_id}"

        context.log.info(f"------************** Updated: this process is running in {env_config['env']} environment. *****************---------")
        context.log.info(f"Using staging table: {raw_schema}.{staging_table_name}")
        context.log.info(f"Checking if etag table exists...")
        _create_etag_table_if_not_exists(context, raw_schema, etag_table_name)

        run_timestamp = pd.Timestamp.now()

        def get_next_page(response):
            link_header = response.headers.get('Link')
            if link_header:
                links = link_header.split(',')
                for link in links:
                    if 'rel="next"' in link:
                        return link.split(';')[0].strip().strip('<>')
            return None

        def get_contributors_for_repo(owner: str, repo_name: str, etag: str | None) -> tuple:
            api_url = f"https://api.github.com/repos/{owner}/{repo_name}/contributors"
            contributors_list = []
            max_retries = 7
            repo_full_name = f"{owner}/{repo_name}"
            headers = {"Authorization": f"Bearer {gh_pat}", "Accept": "application/vnd.github.v3+json"}
            if etag:
                headers["If-None-Match"] = etag
            params = {"per_page": 100, "anon": "true"}

            for attempt in range(max_retries):
                current_url = api_url
                first_page_response_headers = None
                while current_url:
                    try:
                        time.sleep(.5)
                        response = requests.get(current_url, headers=headers, params=params if current_url == api_url else None)
                        if current_url == api_url and response.status_code == 304:
                            return "not_modified", None, etag
                        if current_url == api_url:
                            first_page_response_headers = response.headers
                        response.raise_for_status()
                        if response.status_code == 204:
                            current_url = None
                            continue
                        contributors = response.json()
                        if isinstance(contributors, list):
                            contributors_list.extend(contributors)
                        current_url = get_next_page(response)
                    except requests.exceptions.RequestException as e:
                        status_code = getattr(e.response, 'status_code', None)
                        if status_code == 404:
                            return "failed", None, None
                        if attempt < max_retries - 1:
                            delay = (2 ** attempt) + random.uniform(0, 1)
                            time.sleep(delay)
                            break
                        else:
                            return "failed", None, None
                else:
                    new_etag = first_page_response_headers.get("ETag")
                    return "updated", contributors_list, new_etag
            return "failed", None, None

        # Function to write to a specified staging table
        def write_batch_to_staging_db(batch_data: list, timestamp: pd.Timestamp, table_name: str, schema: str):
            if not batch_data: return 0
            df_batch = pd.DataFrame(batch_data)
            df_batch['data_timestamp'] = timestamp
            try:
                with cloud_sql_engine.begin() as connection:
                    df_batch.to_sql(table_name, connection, if_exists='append', index=False, schema=schema, method='multi')
                context.log.info(f"Successfully wrote batch of {len(df_batch)} rows to staging table '{schema}.{table_name}'.")
                return len(df_batch)
            except SQLAlchemyError as e:
                context.log.error(f"Database error writing batch to staging table: {e}", exc_info=True)
                raise
        
        # --- Main Asset Logic ---
        try:
            # Create the staging table before processing
            with cloud_sql_engine.begin() as conn:
                context.log.info(f"Creating staging table: {raw_schema}.{staging_table_name}")
                # The contributor_list is gzipped bytes, so BYTEA is the correct type for Postgres
                conn.execute(text(f"""
                    CREATE TABLE {raw_schema}.{staging_table_name} (
                        repo VARCHAR,
                        contributor_list BYTEA,
                        data_timestamp TIMESTAMP WITHOUT TIME ZONE
                    );
                """))

            with cloud_sql_engine.connect() as conn:
                query = text(f"SELECT repo FROM {clean_schema}.latest_active_distinct_project_repos WHERE is_active = true AND repo_source = 'github'")
                github_repos = [row[0] for row in conn.execute(query)]

            # get the total number of repos to process for logs
            total_repos = len(github_repos)
            context.log.info(f"Found {total_repos} GitHub repos to process.")

            etags_from_db = _get_etags_from_db(context, raw_schema, etag_table_name, github_repos)
            current_batch, etags_to_update = [], {}
            total_rows_inserted, repos_processed, repos_updated, repos_skipped_304, repos_failed = 0, 0, 0, 0, 0

            LOG_INTERVAL_REPOS = 1000

            for i, repo_url in enumerate(github_repos):
                repos_processed += 1

                if repos_processed % LOG_INTERVAL_REPOS == 0:
                    percentage_complete = (repos_processed / total_repos) * 100
                    context.log.info(f"Progress: {repos_processed}/{total_repos} repos processed ({percentage_complete:.2f}%).")

                try:
                    owner, repo_name = urlparse(repo_url).path.strip("/").split("/")
                    existing_etag = etags_from_db.get(repo_url)
                    status, contributors_list, new_etag = get_contributors_for_repo(owner, repo_name, existing_etag)
                    
                    if status == "updated":
                        repos_updated += 1
                        etags_to_update[repo_url] = new_etag
                        compressed_data = gzip.compress(json.dumps(contributors_list).encode('utf-8'))
                        current_batch.append({"repo": repo_url, "contributor_list": compressed_data})
                    elif status == "not_modified":
                        repos_skipped_304 += 1
                        etags_to_update[repo_url] = new_etag
                    elif status == "failed":
                        repos_failed += 1
                    
                    if len(current_batch) >= BATCH_SIZE:
                        # Write to the staging table
                        total_rows_inserted += write_batch_to_staging_db(current_batch, run_timestamp, staging_table_name, raw_schema)
                        current_batch.clear()
                except Exception as e:
                    repos_failed += 1
                    context.log.error(f"An unexpected error occurred while processing {repo_url}: {e}", exc_info=True)
            
            if current_batch:
                # Write the final batch to the staging table
                total_rows_inserted += write_batch_to_staging_db(current_batch, run_timestamp, staging_table_name, raw_schema)
            
            # Append all data from staging to the final table in a single transaction
            if total_rows_inserted > 0:
                with cloud_sql_engine.begin() as conn:
                    context.log.info(f"Appending {total_rows_inserted} rows from staging table to {raw_schema}.{target_table_name}...")
                    append_sql = text(f"""
                        INSERT INTO {raw_schema}.{target_table_name} (repo, contributor_list, data_timestamp)
                        SELECT repo, contributor_list, data_timestamp FROM {raw_schema}.{staging_table_name};
                    """)
                    conn.execute(append_sql)
                    context.log.info("Append operation successful.")
            else:
                context.log.info("No new rows to append.")

            _update_etags_in_db(context, raw_schema, etag_table_name, etags_to_update)
        
        finally:
            # Ensure the staging table is always dropped
            with cloud_sql_engine.begin() as conn:
                context.log.info(f"Dropping staging table: {raw_schema}.{staging_table_name}")
                conn.execute(text(f"DROP TABLE IF EXISTS {raw_schema}.{staging_table_name};"))

        # --- Metadata Collection ---
        final_row_count = 0
        try:
            with cloud_sql_engine.connect() as conn:
                count_query = text(f"SELECT COUNT(*) FROM {raw_schema}.{target_table_name}")
                final_row_count = conn.execute(count_query).scalar_one_or_none() or 0
        except SQLAlchemyError as e:
            context.log.error(f"Database error fetching final metadata: {e}", exc_info=True)

        return dg.MaterializeResult(
            metadata={
                "Run Timestamp": dg.MetadataValue.text(str(run_timestamp)),
                "Total Repos Processed": dg.MetadataValue.int(repos_processed),
                "Repos Updated (New Data)": dg.MetadataValue.int(repos_updated),
                "Repos Skipped (304 Not Modified)": dg.MetadataValue.int(repos_skipped_304),
                "Repos Failed": dg.MetadataValue.int(repos_failed),
                "Rows Inserted This Run": dg.MetadataValue.int(total_rows_inserted),
                "Total Rows in Table": dg.MetadataValue.int(final_row_count),
            }
        )
    return _github_project_repos_contributors_env_specific


# define the asset that gets basic information about the github contributors in the clean.latest_contributors table
# supplemental data that was not part of the REST response when getting repo contributor data
# this asset also gets the latest and greatest contributor node id from the graphql api
# use graphql api node id
# to accomodate multiple environments, we will use a factory function
def create_latest_contributor_data_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="latest_contributor_data",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        automation_condition=dg.AutomationCondition.eager(),
    )
    def _latest_contributor_data_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        def get_github_contributor_data(node_ids, gh_pat):
            """
            Retrieves detailed GitHub contributor data using the GraphQL API.

            Args:
                node_ids: A list of GitHub contributor node IDs.
                gh_pat: GitHub Personal Access Token with necessary scopes (e.g., read:user, user:email).

            Returns:
                A tuple containing:
                - results (dict): A dictionary mapping each contributor node ID to another dictionary:
                    {
                        "is_active": bool,  # True if the node was found and processed, False otherwise
                        "data": dict or None  # Dictionary of user fields if node is a User and found,
                                            # basic node info if found but not a User,
                                            # or None if node not found or an error occurred.
                    }
                - error_counts (dict): A dictionary with counts of HTTP errors encountered.
                    {
                        "count_403_errors": int,
                        "count_502_errors": int
                    }
                - writes to the table raw.latest_contributor_data
            """

            if not node_ids:  # Handle empty input list
                return {}, {"count_403_errors": 0, "count_502_errors": 0}

            api_url = "https://api.github.com/graphql"
            headers = {"Authorization": f"bearer {gh_pat}"}
            results = {}  # Store results
            batch_size = 100  # Reduced batch size slightly due to more complex query
            cpu_time_used = 0
            real_time_used = 0
            real_time_window = 60 # seconds
            cpu_time_limit = 50   # seconds within the real_time_window
            count_403_errors = 0
            count_502_errors = 0
            batch_time_history = []

            for i in range(0, len(node_ids), batch_size):
                context.log.info(f"Processing batch: {i} - {min(i + batch_size, len(node_ids))}")
                start_time = time.time() # Batch start time
                current_batch_node_ids = node_ids[i:i + batch_size]
                processed_in_batch = set() # Track success/failure per ID for the current batch
                query_definition_parts = []
                query_body_parts = []
                variables = {}

                # 1. Declare variables and construct the query body parts
                for j, node_id_value in enumerate(current_batch_node_ids):
                    variable_name = f"v{j}_nodeId"
                    node_alias = f"n{j}_node"

                    query_definition_parts.append(f"${variable_name}: ID!")
                    variables[variable_name] = node_id_value

                    # Construct the part of the query body for this node
                    # Requesting specific fields for nodes of type User
                    query_body_parts.append(f"""
                        {node_alias}: node(id: ${variable_name}) {{
                            __typename
                            id
                            ... on User {{
                                company
                                email
                                isBountyHunter
                                isHireable
                                location
                                twitterUsername
                                websiteUrl
                                bio
                            }}
                        }}""")

                # 2. Combine parts into the full query
                if not query_definition_parts: # Skip if batch was empty (e.g. if node_ids was an empty list)
                    context.log.info(f"Skipping empty batch: {i} - {min(i + batch_size, len(node_ids))}")
                    continue

                full_query_definition = "query (" + ", ".join(query_definition_parts) + ") {"
                full_query_body = "".join(query_body_parts)
                query = full_query_definition + full_query_body + "\n}"

                # Rate limiting and retry logic
                max_retries = 8
                request_successful_for_batch = False

                for attempt in range(max_retries):
                    context.log.info(f"Batch {i // batch_size + 1}, Attempt: {attempt + 1}")
                    
                    # Simple CPU/Real time throttling (can be made more sophisticated)
                    # This logic might need refinement based on actual GitHub API behavior and observed limits.
                    # The primary rate limit is usually based on points per hour, not CPU seconds.
                    if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                        extra_delay = (cpu_time_used - cpu_time_limit) / 2 # Heuristic
                        extra_delay = max(1, extra_delay) 
                        context.log.info(f"CPU time limit heuristic reached. Delaying for {extra_delay:.2f} seconds.")
                        time.sleep(extra_delay)
                        # Reset window counters after deliberate delay
                        cpu_time_used = 0
                        real_time_used = 0
                        start_time = time.time() # Reset batch timer
                    elif real_time_used >= real_time_window:
                        context.log.info(f"Real time window limit reached. Resetting counters.")
                        cpu_time_used = 0
                        real_time_used = 0
                        # No explicit sleep here, assuming next batch will start a new window.
                        # Or, if this is within a batch retry, the standard retry delay will apply.


                    batch_request_start_time = time.time() # For measuring individual request time
                    
                    try:
                        response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers, timeout=30) # Added timeout
                        response_time = time.time() - batch_request_start_time
                        context.log.info(f"API request time: {response_time:.2f} seconds")

                        # Consistent delay after each request to be polite to the API
                        time.sleep(2.5) 
                        
                        response.raise_for_status() # Check for HTTP errors like 4xx, 5xx
                        data = response.json()

                        if 'errors' in data and data['errors']:
                            is_rate_limited = False
                            for error in data['errors']:
                                context.log.warning(f"GraphQL Error: {error.get('message', str(error))}")
                                if error.get('type') == 'RATE_LIMITED':
                                    is_rate_limited = True
                                    # Try to get 'Retry-After' from GraphQL error extensions if available,
                                    # otherwise use X-RateLimit-Reset header.
                                    retry_after_graphql = error.get('extensions', {}).get('retryAfter')
                                    if retry_after_graphql:
                                        delay = int(retry_after_graphql) + 1 # Add a small buffer
                                        context.log.warning(f"GraphQL Rate Limited. Suggested retry after {delay} seconds.")
                                    elif response.headers.get('X-RateLimit-Reset'):
                                        reset_at = int(response.headers.get('X-RateLimit-Reset'))
                                        delay = max(1, reset_at - int(time.time()) + 1)
                                    else: # Fallback if no specific retry time is given
                                        delay = (2 ** attempt) * 5 + random.uniform(0,1) # Exponential backoff
                                    
                                    delay = min(delay, 300) # Cap delay
                                    context.log.warning(f"Rate limited (GraphQL). Waiting for {delay:.2f} seconds...")
                                    time.sleep(delay)
                                    break # Break from error loop to retry batch
                            if is_rate_limited:
                                continue # Retry the current batch

                        if 'data' in data:
                            # first we have to check if the response has any deprecation warnings
                            # if it does, we need to map the legacy node id to the next node id
                            # this is because the node id is deprecated and we need to use the next node id
                            legacy_to_next_id_map = {}
                            if 'extensions' in data and 'warnings' in data['extensions']:
                                for warning in data['extensions']['warnings']:
                                    if warning.get('type') == 'DEPRECATION' and 'data' in warning:
                                        warn_data = warning.get('data', {})
                                        legacy_id = warn_data.get('legacy_global_id')
                                        next_id = warn_data.get('next_global_id')
                                        if legacy_id and next_id:
                                            legacy_to_next_id_map[legacy_id] = next_id
                            # now process the data
                            for j, node_id in enumerate(current_batch_node_ids):
                                if node_id in processed_in_batch:
                                    continue
                                
                                node_data_from_response = data['data'].get(f"n{j}_node")
                                
                                if node_data_from_response:
                                    id_from_payload = node_data_from_response.get('id') # This is still legacy if legacy was queried
                                    # Determine the canonical ID to store
                                    # Use next_global_id if available for the node_id, else use id_from_payload
                                    canonical_id = legacy_to_next_id_map.get(node_id, id_from_payload)

                                    extracted_info = {
                                        "__typename": node_data_from_response.get('__typename'),
                                        "contributor_node_id": canonical_id
                                    }
                                    if node_data_from_response.get('__typename') == 'User':
                                        extracted_info.update({
                                            "company": node_data_from_response.get("company"),
                                            "email": node_data_from_response.get("email"),
                                            "is_bounty_hunter": node_data_from_response.get("isBountyHunter"),
                                            "is_hireable": node_data_from_response.get("isHireable"),
                                            "location": node_data_from_response.get("location"),
                                            "twitter_username": node_data_from_response.get("twitterUsername"),
                                            "website_url": node_data_from_response.get("websiteUrl"),
                                            "bio": node_data_from_response.get("bio")
                                        })
                                    results[node_id] = {"is_active": True, "data": extracted_info}
                                else:
                                    # Node ID was in query, but no data returned for it (e.g. ID doesn't exist, or permission issue for this specific node)
                                    context.log.warning(f"Data for node_id {node_id} is null or missing in response.")
                                    results[node_id] = {"is_active": False, "data": None} # Mark as inactive if not found
                                
                                processed_in_batch.add(node_id)
                            request_successful_for_batch = True # All nodes in batch processed from response
                            break # Successfully processed batch, exit retry loop

                    except requests.exceptions.HTTPError as e:
                        context.log.warning(f"HTTP error on attempt {attempt + 1} for batch {i // batch_size + 1}: {e}")
                        context.log.warning(f"Status Code: {e.response.status_code if e.response else 'N/A'}")
                        context.log.warning(f"Response content: {e.response.text if e.response else 'N/A'}")

                        rate_limit_info = {
                            'remaining': e.response.headers.get('x-ratelimit-remaining') if e.response else 'N/A',
                            'used': e.response.headers.get('x-ratelimit-used') if e.response else 'N/A',
                            'reset': e.response.headers.get('x-ratelimit-reset') if e.response else 'N/A',
                            'retry_after_header': e.response.headers.get('Retry-After') if e.response else 'N/A'
                        }
                        context.log.warning(f"Rate Limit Info (from headers): {rate_limit_info}")

                        if e.response is not None:
                            if e.response.status_code in (502, 504): # Retry on Bad Gateway/Gateway Timeout
                                count_502_errors +=1
                                delay = (2 ** attempt) * 2 + random.uniform(0, 1) # Exponential backoff
                                context.log.warning(f"502/504 Error. Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                            elif e.response.status_code in (403, 429): # Rate limited by HTTP status
                                count_403_errors += 1
                                retry_after_header = e.response.headers.get('Retry-After')
                                if retry_after_header:
                                    delay = int(retry_after_header) + 1 # Add a small buffer
                                    context.log.warning(f"Rate limited by HTTP {e.response.status_code} (Retry-After header). Waiting for {delay} seconds...")
                                else:
                                    delay = (2 ** attempt) * 5 + random.uniform(0, 1) # Exponential backoff
                                    context.log.warning(f"Rate limited by HTTP {e.response.status_code} (X-RateLimit headers). Waiting for {delay:.2f} seconds...")
                                
                                delay = min(delay, 300) # Cap delay
                                time.sleep(delay)
                                continue
                        # For other HTTP errors, or if max retries reached
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached or unrecoverable HTTP error for batch {i // batch_size + 1}.")
                            break # Exit retry loop for this batch
                        else: # General backoff for other HTTP errors if retrying
                            delay = (2 ** attempt) + random.uniform(0, 1)
                            time.sleep(delay)


                    except requests.exceptions.RequestException as e: # Other network issues (timeout, connection error)
                        context.log.warning(f"RequestException on attempt {attempt + 1} for batch {i // batch_size + 1}: {e}")
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached for RequestException for batch {i // batch_size + 1}.")
                            break
                        delay = (2 ** attempt) * 2 + random.uniform(0, 1) # Exponential backoff
                        context.log.warning(f"Waiting for {delay:.2f} seconds...")
                        time.sleep(delay)
                    
                    except Exception as e: # Catch any other unexpected errors during request/response processing
                        context.log.warning(f"An unexpected error occurred on attempt {attempt + 1} for batch {i // batch_size + 1}: {e}")
                        if attempt == max_retries - 1:
                            context.log.warning(f"Max retries reached due to unexpected error for batch {i // batch_size + 1}.")
                            break
                        # Basic delay, or could break immediately depending on error type
                        time.sleep(5)


                # After all retries for a batch, mark any unprocessed node_ids in that batch as inactive
                if not request_successful_for_batch: # If loop exited due to max_retries or break without success
                    for node_id_in_batch in current_batch_node_ids:
                        if node_id_in_batch not in processed_in_batch:
                            context.log.warning(f"Node {node_id_in_batch} in batch {i // batch_size + 1} failed all retries or was unrecoverable.")
                            results[node_id_in_batch] = {"is_active": False, "data": None}
                
                # Timing and CPU/Real time window update
                batch_processing_time = time.time() - start_time # Total time for batch including retries/delays
                # The `time_since_start` variable from your original code was measuring single request time.
                # `response_time` above measures the actual `requests.post` call.
                # The CPU/Real time logic might need to be based on `response_time` rather than total batch time if
                # the goal is to throttle based on actual API interaction time.
                # For simplicity, using batch_processing_time for real_time_used, and response_time (if available) for cpu_time_used.
                # This part of the original logic is a heuristic and might need careful tuning.
                # Let's assume `cpu_time_used` refers to the time the script was busy processing/waiting for API, not actual CPU cycles.
                # If `response_time` was captured in the last successful try:
                # cpu_time_used += response_time # This is not defined if all attempts failed before getting response_time
                # A simpler approach for the heuristic:
                cpu_time_used += batch_processing_time # Or a fraction of it, if batch_processing_time includes long sleeps
                real_time_used += batch_processing_time

                batch_time_history.append(batch_processing_time)
                if batch_time_history: # Avoid division by zero if list is empty (though it shouldn't be here)
                    # Only print average if more than a few batches processed for meaningful avg
                    if len(batch_time_history) > 3:
                        context.log.info(f"Average batch processing time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
                context.log.info(f"Batch {i // batch_size + 1} completed. Total node_ids to process: {len(node_ids)}")
                context.log.info(f"Time taken to process batch: {batch_processing_time:.2f} seconds")
                context.log.info(f"Cumulative 'CPU time used' heuristic in window: {cpu_time_used:.2f} seconds")
                context.log.info(f"Cumulative 'Real time used' in window: {real_time_used:.2f} seconds")
                context.log.info("-" * 30)


            error_counts = {"count_403_errors": count_403_errors, "count_502_errors": count_502_errors}
            return results, error_counts

        # Execute the query
        with cloud_sql_engine.connect() as conn:

            # query the latest_distinct_project_repos table to get the distinct repo list
            result = conn.execute(
                text(f"""select distinct contributor_node_id, contributor_unique_id_builder_love from {clean_schema}.latest_contributors where contributor_node_id is not null""")
                )
            distinct_contributor_node_ids_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # check if df is a df and not empty
        # if it is raise an error to the dagster context ui
        if not isinstance(distinct_contributor_node_ids_df, pd.DataFrame) or distinct_contributor_node_ids_df.empty:
            context.log.error("no contributor node ids found in builder love database")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        # get the list of node_ids for sending to the function
        node_ids = distinct_contributor_node_ids_df['contributor_node_id'].tolist()

        # get github pat
        gh_pat = github_api.get_client(config.key_name)

        # check if gh_pat is not None
        if gh_pat is None:
            context.log.warning("no github pat found")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        results = get_github_contributor_data(node_ids, gh_pat)

        # check if results is empty
        # if it is raise an error to the dagster context ui
        if not results:
            context.log.error("no results returned by get_github_contributor_data function")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        contributor_results = results[0]
        count_http_errors = results[1]

        # write results to pandas dataframe
        processed_rows = []
        for node_id, result_item in contributor_results.items():
            # Start with the top-level information
            row_data = {
                "is_active": result_item["is_active"],
                "contributor_node_id_legacy": node_id
            }
            # If the 'data' field exists and is a dictionary, unpack its contents
            nested_data = result_item.get("data")
            if isinstance(nested_data, dict):
                data_to_add = nested_data.copy()
                data_to_add.pop('__typename', None) # Removes the '__typename' from the nested dict before updating
                row_data.update(data_to_add)
            processed_rows.append(row_data)

        contributor_results_df = pd.DataFrame(processed_rows)
        
        # check if contributor_results_df is a df and not empty
        # if it is raise an error to the dagster context ui
        if not isinstance(contributor_results_df, pd.DataFrame) or contributor_results_df.empty:
            context.log.error("no contributor results found")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        # add unix datetime column
        contributor_results_df['data_timestamp'] = pd.Timestamp.now()

        # add the contributor_unique_id_builder_love column to the contributor_results_df
        final_df = pd.merge(
        contributor_results_df,
        distinct_contributor_node_ids_df[['contributor_node_id', 'contributor_unique_id_builder_love']],
        left_on="contributor_node_id_legacy", # Key from contributor_results_df
        right_on="contributor_node_id",                # Key from distinct_contributor_node_ids_df (legacy ID)
        how="left",                                    # Use 'left' to keep all rows from contributor_results_df
            suffixes=('', '_from_mapping')                 # Suffix for overlapping column names from right table if any (other than key)
        )

        # drop the contributor_node_id_legacy column
        final_df = final_df.drop(columns=['contributor_node_id_from_mapping'])

        # print info about the contributor_results_df
        context.log.info(f"contributor_results_df:\n {final_df.info()}")
        # print first 5 rows as string
        context.log.info(f"contributor_results_df:\n {final_df.head().to_string()}")

        # write the data to the latest_inactive_contributors table
        # use truncate and append to avoid removing indexes
        try:
            with cloud_sql_engine.connect() as conn:
                with conn.begin():
                    # first truncate the table, idempotently
                    # This ensures the table is empty if it exists, 
                    # and does nothing (without error) if it doesn't exist.
                    context.log.info("writing to latest_contributor_data table. First truncating the table, if exists. Then appending the data, else creating the table.")
                    idempotent_truncate_sql = f"""
                    DO $$
                    BEGIN
                    IF EXISTS (
                        SELECT FROM pg_catalog.pg_tables
                        WHERE  schemaname = '{raw_schema}' -- Schema name in the catalog query
                        AND    tablename  = 'latest_contributor_data'
                    ) THEN
                        TRUNCATE TABLE {raw_schema}.latest_contributor_data;
                        RAISE NOTICE 'Table {raw_schema}.latest_contributor_data truncated.';
                    ELSE
                        RAISE NOTICE 'Table {raw_schema}.latest_contributor_data does not exist, no truncation needed.';
                    END IF;
                    END $$;
                    """
                    conn.execute(text(idempotent_truncate_sql))
                    context.log.info(f"Ensured table {raw_schema}.latest_contributor_data is ready for new data (truncated if existed).")
                    # then append the data
                    final_df.to_sql('latest_contributor_data', conn, if_exists='append', index=False, schema=raw_schema)
                    context.log.info("Table load successful.")
        except Exception as e:
            context.log.error(f"error writing to latest_contributor_data table: {e}")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        # # capture asset metadata
        with cloud_sql_engine.connect() as conn:
            preview_query = text(f"select count(*) from {raw_schema}.latest_contributor_data")
            result = conn.execute(preview_query)
            # Fetch all rows into a list of tuples
            row_count = result.fetchone()[0]

            preview_query = text(f"select * from {raw_schema}.latest_contributor_data limit 10")
            result = conn.execute(preview_query)
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                "count_http_403_errors": dg.MetadataValue.int(count_http_errors['count_403_errors']),
                "count_http_502_errors": dg.MetadataValue.int(count_http_errors['count_502_errors']),
            }
        )

    return _latest_contributor_data_env_specific


###################################################################################
### get github contributor follower count
###################################################################################

def get_github_contributor_followers_count(context, node_ids, gh_pat): # Renamed for clarity
    """
    Retrieves GitHub user ID and their total follower count,
    using the GitHub GraphQL API.

    Args:
        context: Dagster context.
        node_ids: A list of GitHub contributor node IDs.
        gh_pat: GitHub Personal Access Token with necessary scopes (e.g., read:user).

    Returns:
        A tuple containing:
        - results (dict): A dictionary mapping each contributor node ID to their follower count.
        - error_counts (dict): A dictionary with counts of HTTP errors encountered.
    """
    context.log.info(f"Starting to fetch follower counts for {len(node_ids)} contributors.") # Use context.log

    if not node_ids:
        return {}, {"count_403_errors": 0, "count_502_errors": 0, "count_other_errors": 0}

    api_url = "https://api.github.com/graphql"
    headers = {"Authorization": f"bearer {gh_pat}"}
    results = {}
    batch_size = 100 # GraphQL allows up to 100 aliases, but 50 is safer for stability/complexity
    error_counts = {"count_403_errors": 0, "count_502_errors": 0, "count_other_errors": 0}

    for i in range(0, len(node_ids), batch_size):
        current_batch_node_ids = node_ids[i:i + batch_size]
        batch_number = i // batch_size + 1
        context.log.info(f"Processing batch: {batch_number} ({i} - {min(i + batch_size, len(node_ids)) -1} of {len(node_ids)-1})")

        query_definition_parts = []
        query_body_parts = []
        variables = {}

        for j, node_id_value in enumerate(current_batch_node_ids):
            variable_name = f"v{j}_nodeId"
            node_alias = f"n{j}_node"
            query_definition_parts.append(f"${variable_name}: ID!")
            variables[variable_name] = node_id_value
            query_body_parts.append(f"""
                {node_alias}: node(id: ${variable_name}) {{
                    __typename
                    id # This is the primary ID of the user node being queried
                    ... on User {{
                        followers {{
                            totalCount # Only fetch totalCount
                        }}
                    }}
                }}""")

        if not query_definition_parts:
            continue

        full_query_definition = "query (" + ", ".join(query_definition_parts) + ") {"
        full_query_body = "".join(query_body_parts)
        query = full_query_definition + full_query_body + "\n}"

        max_retries_main_batch = 5
        request_successful_for_batch = False

        for attempt in range(max_retries_main_batch):
            context.log.info(f"  Batch {batch_number}, Main Request Attempt: {attempt + 1}")
            try:
                response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers, timeout=60)
                time.sleep(1.0 + random.uniform(0, 0.5)) # Basic sleep after each request

                response.raise_for_status()
                data = response.json()

                if 'errors' in data and data['errors']:
                    is_rate_limited = False
                    for error in data['errors']:
                        context.log.warning(f"  GraphQL Error (Batch {batch_number}): {error.get('message', str(error))}")
                        if error.get('type') == 'RATE_LIMITED':
                            is_rate_limited = True
                            # Basic exponential backoff for GraphQL rate limits
                            delay = (2 ** attempt) * 5 + random.uniform(0,1)
                            delay = min(delay, 300) # Cap delay
                            context.log.warning(f"  Rate limited (GraphQL Batch {batch_number}). Waiting {delay:.2f}s...")
                            time.sleep(delay)
                            break # Break from errors loop, retry the request
                    if is_rate_limited:
                        continue # Continue to next attempt in retry loop

                if 'data' in data:
                    for j_node_idx, node_id in enumerate(current_batch_node_ids):
                        node_data_from_response = data['data'].get(f"n{j_node_idx}_node")
                        if node_data_from_response and node_data_from_response.get('__typename') == 'User':
                            followers_data = node_data_from_response.get("followers")
                            total_followers = 0
                            if followers_data and isinstance(followers_data.get("totalCount"), int):
                                total_followers = followers_data["totalCount"]
                            results[node_id] = {
                                "id": node_data_from_response.get('id'), # Store the user's ID
                                "followers_total_count": total_followers
                            }
                        elif node_data_from_response: # E.g., an Organization or other type
                             results[node_id] = {
                                "id": node_data_from_response.get('id'),
                                "followers_total_count": 0 # Or None, or specific handling
                            }
                        else:
                            results[node_id] = {"id": node_id, "followers_total_count": None} # Error or not found
                    request_successful_for_batch = True
                    break # Break from retry loop, batch successful

            except requests.exceptions.HTTPError as e:
                context.log.warning(f"  HTTP error (Batch {batch_number}, attempt {attempt + 1}): {e}")
                if e.response is not None:
                    if e.response.status_code in (502, 504):
                        error_counts["count_502_errors"] +=1
                        delay = (2 ** attempt) * 3 + random.uniform(0,1) # Slightly more patient for 502s
                        delay = min(delay, 180)
                        context.log.warning(f"  Server error {e.response.status_code}. Retrying in {delay:.2f}s...")
                        time.sleep(delay)
                        continue
                    elif e.response.status_code in (403, 429):
                        error_counts["count_403_errors"] += 1
                        delay = (2 ** attempt) * 5 + random.uniform(0,1)
                        delay = min(delay, 300)
                        context.log.warning(f"  Rate limit/Auth error {e.response.status_code}. Retrying in {delay:.2f}s...")
                        time.sleep(delay)
                        continue
                if attempt == max_retries_main_batch - 1:
                    context.log.error(f"  Max retries for HTTP error in batch {batch_number}. Giving up on this batch.")
                    # Mark all nodes in this batch as failed for this attempt
                    for node_id_in_batch_on_fail in current_batch_node_ids:
                        if node_id_in_batch_on_fail not in results: # Only if not already processed
                             results[node_id_in_batch_on_fail] = {"id": node_id_in_batch_on_fail, "followers_total_count": None}
                    break # Break from retry loop
            except requests.exceptions.RequestException as e_req:
                context.log.warning(f"  RequestException (Batch {batch_number}, attempt {attempt + 1}): {e_req}")
                if attempt == max_retries_main_batch - 1:
                    context.log.error(f"  Max retries for RequestException in batch {batch_number}. Giving up on this batch.")
                    for node_id_in_batch_on_fail in current_batch_node_ids:
                        if node_id_in_batch_on_fail not in results:
                             results[node_id_in_batch_on_fail] = {"id": node_id_in_batch_on_fail, "followers_total_count": None}
                    break
                time.sleep((2**attempt) * 2 + random.uniform(0,1)) # Basic backoff
            except Exception as e_unexpected:
                context.log.error(f"  Unexpected error (Batch {batch_number}, attempt {attempt + 1}): {e_unexpected}", exc_info=True)
                error_counts["count_other_errors"] += 1
                if attempt == max_retries_main_batch - 1:
                    context.log.error(f"  Max retries for Unexpected error in batch {batch_number}. Giving up on this batch.")
                    for node_id_in_batch_on_fail in current_batch_node_ids:
                        if node_id_in_batch_on_fail not in results:
                             results[node_id_in_batch_on_fail] = {"id": node_id_in_batch_on_fail, "followers_total_count": None}
                    break
                time.sleep(5) # Basic sleep before retry

        if not request_successful_for_batch:
            context.log.warning(f"Batch {batch_number} ultimately failed after all retries.")
            # Ensure all nodes in a failed batch have a placeholder if not already set
            for node_id_in_batch_final_fail in current_batch_node_ids:
                if node_id_in_batch_final_fail not in results:
                    results[node_id_in_batch_final_fail] = {"id": node_id_in_batch_final_fail, "followers_total_count": None} # Indicate failure/no data

        context.log.info(f"Batch {batch_number} completed processing.")
        context.log.info("-" * 40) # Keep for visual separation in logs if desired

    context.log.info(f"Finished fetching follower counts. Processed {len(results)} contributors.")
    return results, error_counts


# Main function to retrieve the list of ALL contributor followers (paginated)
# define the asset that gets followers for github contributors in the clean.latest_contributor_data table
# use graphql api node id
# to accomodate multiple environments, we will use a factory function
def create_contributor_follower_count_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="contributor_follower_count",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},  
        group_name="ingestion",
        tags={"github_api": "True"}
    )
    def _contributor_follower_count_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult: # Renamed asset
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        context.log.info("Starting contributor_follower_count asset.")
        fallback_filename = f"/tmp/contributor_follower_count_fallback_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        gh_pat = github_api.get_client(config.key_name)

        if gh_pat is None:
            context.log.warning("No GitHub PAT found.")
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})

        with cloud_sql_engine.connect() as conn:
            # Your existing query to get distinct contributor node IDs
            # (Ensure this query is efficient for 275,000+ potential contributors if the source table is huge)
            distinct_contributor_node_ids_df = pd.read_sql_query(
                text(
                    f"""
                        select distinct lcd.contributor_node_id
                        from {clean_schema}.latest_contributor_data lcd left join {clean_schema}.latest_contributor_activity lca
                            on lcd.contributor_node_id = lca.contributor_node_id left join {clean_schema}.latest_contributors lc
                            on lcd.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love
                        where lcd.contributor_node_id is not null
                        and lca.has_contributed_in_last_year = true
                        and lcd.is_active = true
                        and lower(lc.contributor_type) not in('bot', 'anonymous')
                    """
                ),
                conn
            )
        context.log.info(f"Fetched {len(distinct_contributor_node_ids_df)} distinct contributor node IDs from the database. Starting to fetch follower counts.")

        if distinct_contributor_node_ids_df.empty:
            context.log.error("No contributor node IDs found in the database that meet the criteria.")
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})

        node_ids = distinct_contributor_node_ids_df['contributor_node_id'].tolist()

        # Using the simplified function name
        api_results, count_http_errors = get_github_contributor_followers_count(context, node_ids, gh_pat)

        if not api_results:
            context.log.error("No results returned by get_github_contributor_followers_count function.")
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})

        processed_rows = []
        for contributor_node_id, result_item in api_results.items():
            if result_item and isinstance(result_item.get("followers_total_count"), int): # Check for successful fetch
                row_data = {
                    "contributor_node_id": contributor_node_id,
                    "followers_total_count": result_item["followers_total_count"],
                }
                processed_rows.append(row_data)
            else:
                # Log or handle contributors for whom follower count couldn't be fetched
                context.log.warning(f"Could not retrieve follower count for contributor_node_id: {contributor_node_id}. Result: {result_item}")


        if not processed_rows:
            context.log.error("No contributor follower counts processed successfully.")
            # If you want to materialize an empty table or just log, adjust here
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})

        contributor_followers_counts_df = pd.DataFrame(processed_rows)
        context.log.info(f"Created DataFrame with {len(contributor_followers_counts_df)} rows of follower counts.")

        if contributor_followers_counts_df.empty:
            context.log.warning("Follower counts DataFrame is empty after processing API results.")
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})
            
        contributor_followers_counts_df['data_timestamp'] = pd.Timestamp.now()

        # lookup and swap legacy contributor_node_id with new contributor_node_id format
        contributor_followers_counts_df = contributor_node_id_swap(context, contributor_followers_counts_df, cloud_sql_engine)
        context.log.info("Lookup and swap legacy contributor_node_id with new contributor_node_id format.")

        if contributor_followers_counts_df.empty: # Re-check after potential swap
            context.log.error("DataFrame became empty after node ID swap.")
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})

        context.log.info(f"Final contributor_followers_counts_df to load:\n{contributor_followers_counts_df.head().to_markdown(index=False)}")

        # Define table name carefully - this implies a new table structure
        table_name = 'contributor_follower_count' # Suggesting a new table name
        db_schema = raw_schema

        try:
            with cloud_sql_engine.connect() as conn:
                with conn.begin():
                    context.log.info(f"Attempting to load new data to table {db_schema}.{table_name}.")
                    contributor_followers_counts_df.to_sql(
                        table_name,
                        conn,
                        if_exists='append',
                        chunksize=50000,
                        index=False,
                        schema=db_schema
                    )
                    context.log.info(f"Table {db_schema}.{table_name} load successful.")
        except Exception as e:
            context.log.error(f"Error writing to {db_schema}.{table_name}: {e}", exc_info=True)
            try:
                contributor_followers_counts_df.to_parquet(fallback_filename, index=False)
                context.log.info(f"Fallback Parquet file saved to: {fallback_filename}")
            except Exception as e_parquet:
                context.log.error(f"Error writing fallback Parquet file: {e_parquet}", exc_info=True)
            # Return a failure or partial success indicator if desired
            return dg.MaterializeResult(
                metadata={
                    "row_count": dg.MetadataValue.int(0),
                    "error": dg.MetadataValue.text(str(e))
                }
            )

        # Metadata capture
        row_count = len(contributor_followers_counts_df)
        preview_df = contributor_followers_counts_df.head()

        context.log.info(f"Asset materialization complete. Total rows: {row_count}")
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "count_http_403_errors": dg.MetadataValue.int(count_http_errors.get('count_403_errors', 0)),
                "count_http_502_errors": dg.MetadataValue.int(count_http_errors.get('count_502_errors', 0)),
                "count_other_errors": dg.MetadataValue.int(count_http_errors.get('count_other_errors', 0)),
            }
        )

    return _contributor_follower_count_env_specific


###################################################################################
### get github contributor following count
###################################################################################

def get_github_contributor_following_count(context, node_ids, gh_pat):
    """
    Retrieves GitHub user ID and a list of ALL their following (paginated),
    using the GitHub GraphQL API.

    Args:
        node_ids: A list of GitHub contributor node IDs.
        gh_pat: GitHub Personal Access Token with necessary scopes (e.g., read:user).

    Returns:
        A count of the number of following for each contributor node ID.
    """

    if not node_ids:
        return {}, {"count_403_errors": 0, "count_502_errors": 0}

    api_url = "https://api.github.com/graphql"
    headers = {"Authorization": f"bearer {gh_pat}"}
    results = {}
    batch_size = 150
    
    cpu_time_used = 0 
    real_time_used = 0
    real_time_window = 60 
    cpu_time_limit = 50   
    error_counts = {"count_403_errors": 0, "count_502_errors": 0} 
    batch_time_history = []

    for i in range(0, len(node_ids), batch_size):
        context.log.info(f"Processing batch: {i // batch_size + 1} ({i} - {min(i + batch_size, len(node_ids)) -1} of {len(node_ids)-1})")
        batch_start_time = time.time()
        current_batch_node_ids = node_ids[i:i + batch_size]
        processed_in_batch = set()
        query_definition_parts = []
        query_body_parts = []
        variables = {}

        for j, node_id_value in enumerate(current_batch_node_ids):
            variable_name = f"v{j}_nodeId"
            node_alias = f"n{j}_node"
            query_definition_parts.append(f"${variable_name}: ID!")
            variables[variable_name] = node_id_value
            # Followers will still have id and login.
            query_body_parts.append(f"""
                {node_alias}: node(id: ${variable_name}) {{
                    __typename
                    id # This is the primary ID of the user node being queried
                    ... on User {{
                        following(first: 1) {{
                            totalCount
                        }}
                    }}
                }}""")

        if not query_definition_parts:
            continue

        full_query_definition = "query (" + ", ".join(query_definition_parts) + ") {"
        full_query_body = "".join(query_body_parts)
        query = full_query_definition + full_query_body + "\n}"

        max_retries_main_batch = 5 
        request_successful_for_batch = False

        for attempt in range(max_retries_main_batch):
            context.log.info(f"  Batch {i // batch_size + 1}, Main Request Attempt: {attempt + 1}")
            
            if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                delay = max(1, (cpu_time_used - cpu_time_limit) / 2)
                context.log.warning(f"  CPU time heuristic. Delaying main batch for {delay:.2f}s.")
                time.sleep(delay)
                cpu_time_used = real_time_used = 0
            elif real_time_used >= real_time_window:
                cpu_time_used = real_time_used = 0
            
            batch_req_start_time_inner = time.time()
            try:
                response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers, timeout=60) 
                response_time = time.time() - batch_req_start_time_inner
                context.log.info(f"  Main batch API request time: {response_time:.2f} seconds")
                time.sleep(2.0) 
                
                response.raise_for_status()
                data = response.json()

                if 'errors' in data and data['errors']:
                    is_rate_limited = False
                    for error in data['errors']:
                        context.log.warning(f"  GraphQL Error (Main Batch): {error.get('message', str(error))}")
                        if error.get('type') == 'RATE_LIMITED':
                            is_rate_limited = True
                            retry_after_graphql = error.get('extensions', {}).get('retryAfter')
                            if retry_after_graphql: 
                                delay = int(retry_after_graphql) + 1
                            elif response.headers.get('X-RateLimit-Reset'): 
                                delay = max(1, int(response.headers.get('X-RateLimit-Reset')) - int(time.time()) + 1)
                            else: 
                                delay = (2 ** attempt) * 5 + random.uniform(0,1)
                            delay = min(delay, 300)
                            context.log.warning(f"  Rate limited (GraphQL Main Batch). Waiting {delay:.2f}s...")
                            time.sleep(delay)
                            break 
                    if is_rate_limited: continue

                if 'data' in data:
                    for j_node_idx, node_id in enumerate(current_batch_node_ids):
                        if node_id in processed_in_batch: continue
                        
                        node_data_from_response = data['data'].get(f"n{j_node_idx}_node")
                        if node_data_from_response:
                            if node_data_from_response.get('__typename') == 'User':
                                # get followers data
                                following_data = node_data_from_response.get("following")
                                if following_data:
                                    results[node_id] = {
                                        "__typename": node_data_from_response.get('__typename'),
                                        "id": node_data_from_response.get('id'),
                                        "following_total_count": following_data.get("totalCount", 0)
                                    }
                                else:
                                    results[node_id] = {
                                        "__typename": node_data_from_response.get('__typename'),
                                        "id": node_data_from_response.get('id'),
                                        "following_total_count": 0
                                    }
                        else:
                            results[node_id] = {
                                "__typename": None,
                                "id": node_id,
                                "following_total_count": 0
                            }
                        processed_in_batch.add(node_id)
                    request_successful_for_batch = True 
                    break 
            except requests.exceptions.HTTPError as e:
                context.log.warning(f"  HTTP error (Main Batch {i // batch_size + 1}): {e}")
                if e.response is not None:
                    if e.response.status_code in (502, 504): 
                        error_counts["count_502_errors"] +=1
                        delay = (2 ** attempt) * 2 + random.uniform(0,1) 
                        time.sleep(delay)
                        continue
                    elif e.response.status_code in (403, 429): 
                        error_counts["count_403_errors"] += 1
                        retry_after_header = e.response.headers.get('Retry-After')
                        if retry_after_header: 
                            delay = int(retry_after_header) + 1
                        else: 
                            delay = (2 ** attempt) * 5 + random.uniform(0,1)
                        time.sleep(min(delay,300))
                        continue
                if attempt == max_retries_main_batch - 1: 
                    break
                else: 
                    time.sleep((2 ** attempt) + random.uniform(0,1))
            except requests.exceptions.RequestException as e:
                context.log.warning(f"  RequestException (Main Batch {i // batch_size + 1}): {e}")
                if attempt == max_retries_main_batch - 1: 
                    break
                time.sleep((2 ** attempt) * 2 + random.uniform(0,1))
            except Exception as e:
                context.log.warning(f"  Unexpected error (Main Batch {i // batch_size + 1}): {e}")
                context.log.warning(traceback.format_exc())
                if attempt == max_retries_main_batch - 1: 
                    break
                time.sleep(5)

        if not request_successful_for_batch: 
            for node_id_in_batch in current_batch_node_ids:
                if node_id_in_batch not in processed_in_batch:
                    results[node_id_in_batch] = {"data": None}
            
        batch_processing_time = time.time() - batch_start_time
        cpu_time_used += batch_processing_time 
        real_time_used += batch_processing_time

        batch_time_history.append(batch_processing_time)
        if len(batch_time_history) > 1: 
            context.log.info(f"  Average total batch processing time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
        context.log.info(f"Batch {i // batch_size + 1} completed. Time: {batch_processing_time:.2f}s. CPU heuristic: {cpu_time_used:.2f}s. Real time: {real_time_used:.2f}s.")
        context.log.info("-" * 40)
    
    return results, error_counts


# Main function to retrieve the list of ALL contributors a contributor is following
# define the asset that gets following for github contributors in the clean.latest_contributor_data table
# use graphql api node id
# to accomodate multiple environments, we will use a factory function
def create_latest_contributor_following_count_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="latest_contributor_following_count",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api"},
        group_name="ingestion",
        tags={"github_api": "True"}
    )
    def _latest_contributor_following_count_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        # Define a fallback filename (consider making it unique per run)
        fallback_filename = f"/tmp/contributors_following_fallback_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"

        # Get the cloud sql postgres resource
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource

        # define the github pat
        gh_pat = github_api.get_client(config.key_name)

        # check if gh_pat is not None
        if gh_pat is None:
            context.log.warning("no github pat found")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )
        
        # Execute the query
        with cloud_sql_engine.connect() as conn:

            # query the latest_distinct_project_repos table to get the distinct repo list
            result = conn.execute(
                text(
                    f"""
                        select distinct lcd.contributor_node_id
                        from {clean_schema}.latest_contributor_data lcd left join {clean_schema}.latest_contributor_activity lca
                            on lcd.contributor_node_id = lca.contributor_node_id left join {clean_schema}.latest_contributors lc
                            on lcd.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love
                        where lcd.contributor_node_id is not null
                        and lca.has_contributed_in_last_year = true
                        and lcd.is_active = true
                        and lower(lc.contributor_type) not in('bot', 'anonymous')
                    """
                    )
                )
            distinct_contributor_node_ids_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # check if df is a df and not empty
        # if it is raise an error to the dagster context ui
        if not isinstance(distinct_contributor_node_ids_df, pd.DataFrame) or distinct_contributor_node_ids_df.empty:
            context.log.error("no contributor node ids found in builder love database")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        # get the list of node_ids for sending to the function
        node_ids = distinct_contributor_node_ids_df['contributor_node_id'].tolist()

        results = get_github_contributor_following_count(context, node_ids, gh_pat)

        # check if results is empty
        # if it is raise an error to the dagster context ui
        if not results:
            context.log.error("no results returned by get_github_contributor_following function")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        contributor_results = results[0]
        count_http_errors = results[1]

        # write results to pandas dataframe
        processed_rows = []

        for original_queried_node_id, api_data_for_node in contributor_results.items():
            # api_data_for_node is the dictionary like {"__typename": ..., "id": ..., "following_total_count": ...}

            # The 'id' field from api_data_for_node is the GitHub ID. 
            # For this flattening script, we'll assume api_data_for_node["id"] is the ID you want to store
            # as 'contributor_node_id'.
            contributor_id_for_df = api_data_for_node.get("id", original_queried_node_id) # Fallback to original if 'id' key is missing

            total_following = api_data_for_node.get("following_total_count", 0)

            row_data = {
                "contributor_node_id": contributor_id_for_df, # This is the user whose stats these are
                "total_following_count": total_following
            }
            processed_rows.append(row_data)

        # Create the DataFrame
        contributor_following_df = pd.DataFrame(processed_rows)
        
        # check if contributor_results_df is a df and not empty
        # if it is raise an error to the dagster context ui
        if not isinstance(contributor_following_df, pd.DataFrame) or contributor_following_df.empty:
            context.log.error("no contributor results found")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        # add unix datetime column
        contributor_following_df['data_timestamp'] = pd.Timestamp.now()

        # swap the github legacy contributor node id for the new format contributor node id
        contributor_following_df = contributor_node_id_swap(context, contributor_following_df, cloud_sql_engine)

        # check results of swap
        if not isinstance(contributor_following_df, pd.DataFrame) or contributor_following_df.empty:
            context.log.error("no contributor results found after swapping legacy contributor node id for new format contributor node id")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        # print info about the contributor_results_df
        context.log.info(f"contributor_following_df:\n {contributor_following_df.info()}")

        # write the data to the latest_contributor_following table
        # use truncate and append to avoid removing indexes
        try:
            with cloud_sql_engine.connect() as conn:
                with conn.begin():
                    # first truncate the table, idempotently
                    # This ensures the table is empty if it exists, 
                    # and does nothing (without error) if it doesn't exist.
                    context.log.info(f"writing to {raw_schema}.latest_contributor_following table. First truncating the table, if exists. Then appending the data, else creating the table.")
                    idempotent_truncate_sql = f"""
                    DO $$
                    BEGIN
                    IF EXISTS (
                        SELECT FROM pg_catalog.pg_tables
                        WHERE  schemaname = '{raw_schema}' -- Schema name in the catalog query
                        AND    tablename  = 'latest_contributor_following'
                    ) THEN
                        TRUNCATE TABLE {raw_schema}.latest_contributor_following;
                        RAISE NOTICE 'Table {raw_schema}.latest_contributor_following truncated.';
                    ELSE
                        RAISE NOTICE 'Table {raw_schema}.latest_contributor_following does not exist, no truncation needed.';
                    END IF;
                    END $$;
                    """
                    conn.execute(text(idempotent_truncate_sql))
                    context.log.info(f"Ensured table {raw_schema}.latest_contributor_following is ready for new data (truncated if existed).")

                    # then append the data
                    contributor_following_df.to_sql('latest_contributor_following', conn, if_exists='append', index=False, schema=raw_schema)
                    context.log.info("Table load successful.")
        except Exception as e:
            context.log.error(f"error writing to latest_contributor_following table: {e}")
            try:
                # Attempt to save as Parquet
                contributor_following_df.to_parquet(fallback_filename, index=False)
                context.log.info(f"Fallback Parquet file saved to: {fallback_filename}")
            except Exception as e:
                context.log.error(f"error writing to latest_contributor_following table: {e}")
            return dg.MaterializeResult(
                metadata={"row_count": dg.MetadataValue.int(0)}
            )

        # # capture asset metadata
        with cloud_sql_engine.connect() as conn:
            preview_query = text(f"select count(*) from {raw_schema}.latest_contributor_following")
            result = conn.execute(preview_query)
            # Fetch all rows into a list of tuples
            row_count = result.fetchone()[0]

            preview_query = text(f"select * from {raw_schema}.latest_contributor_following limit 10")
            result = conn.execute(preview_query)
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                "count_http_403_errors": dg.MetadataValue.int(count_http_errors['count_403_errors']),
                "count_http_502_errors": dg.MetadataValue.int(count_http_errors['count_502_errors']),
            }
        )

    return _latest_contributor_following_count_env_specific


###################################################################################
### get github user recent activity - 1 year
###################################################################################

def get_github_user_latest_activity(context, node_ids, gh_pat):
    """
    Retrieves GitHub user ID, login, and their latest activity (aiming for top 100)
    using the GitHub GraphQL API.
    """
    if not node_ids:
        return {}, {"count_403_errors": 0, "count_502_errors": 0}

    api_url = "https://api.github.com/graphql"
    headers = {"Authorization": f"bearer {gh_pat}"}
    results = {}
    batch_size = 20 # Reduced batch_size as a precaution, adjust as needed
    
    error_counts = {"count_403_errors": 0, "count_502_errors": 0} 
    batch_time_history = []

    # Define date range for contributionsCollection (e.g., last year)
    now_utc = datetime.now(timezone.utc)
    one_year_ago_utc = now_utc - pd.Timedelta(days=365) # Contributions from the last year
    
    for i in range(0, len(node_ids), batch_size):
        context.log.info(f"Processing batch for latest activity: {i // batch_size + 1} ({i} - {min(i + batch_size, len(node_ids)) -1} of {len(node_ids)-1})")
        batch_start_time = time.time()
        current_batch_node_ids = node_ids[i:i + batch_size]
        processed_in_batch = set()
        
        query_definition_parts = ["$contributionFromDate: DateTime!", "$contributionToDate: DateTime!"]
        query_body_parts = []
        variables = {
            "contributionFromDate": one_year_ago_utc.isoformat(),
            "contributionToDate": now_utc.isoformat()
        }

        for j, node_id_value in enumerate(current_batch_node_ids):
            variable_name = f"v{j}_nodeId"
            node_alias = f"n{j}_node"
            query_definition_parts.append(f"${variable_name}: ID!")
            variables[variable_name] = node_id_value
            
            query_body_parts.append(f"""
                {node_alias}: node(id: ${variable_name}) {{
                    __typename
                    id
                    ... on User {{
                        contributionsCollection(from: $contributionFromDate, to: $contributionToDate) {{
                            hasAnyContributions
                        }}
                    }}
                }}""")

        if not query_body_parts: continue # Should not happen if current_batch_node_ids is not empty
        full_query_definition = "query (" + ", ".join(query_definition_parts) + ") {"
        full_query_body = "".join(query_body_parts)
        query = full_query_definition + full_query_body + "\n}"

        max_retries_main_batch = 10 
        request_successful_for_batch = False

        for attempt in range(max_retries_main_batch):
            context.log.info(f"  Batch {i // batch_size + 1}, Main Request Attempt: {attempt + 1}")
            batch_req_start_time_inner = time.time()
            try:
                response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers, timeout=90)
                response_time = time.time() - batch_req_start_time_inner
                context.log.info(f"  Main batch API request time: {response_time:.2f} seconds")
                # Optional: Shorter sleep if requests are simpler, but keep some delay
                time.sleep(1.0 + random.uniform(0, 0.5)) 
                
                response.raise_for_status()
                data = response.json()

                if 'errors' in data and data['errors']:
                    is_rate_limited = False
                    for error_item in data['errors']:
                        context.log.warning(f"  GraphQL Error (Main Batch): {error_item.get('message', str(error_item))}")
                        if error_item.get('type') == 'RATE_LIMITED': is_rate_limited = True; break
                    if is_rate_limited: 
                        delay = (2 ** attempt) * 10 + random.uniform(0,1)
                        context.log.info(f"  Rate limited. Sleeping for {delay} seconds.")
                        time.sleep(min(delay, 300))
                        continue
                
                if 'data' in data:
                    for j_node_idx, node_id in enumerate(current_batch_node_ids):
                        if node_id in processed_in_batch: continue
                        
                        node_data_from_response = data['data'].get(f"n{j_node_idx}_node")
                        if node_data_from_response:
                            extracted_info = {
                                "__typename": node_data_from_response.get('__typename'),
                                "id": node_data_from_response.get('id')
                            }
                            
                            latest_contribution_details = None # Initialize for this user

                            if node_data_from_response.get('__typename') == 'User':
                                contrib_collection = node_data_from_response.get("contributionsCollection")
                                if contrib_collection:

                                    latest_contribution_details = {
                                        # spread the extracted info into the latest_contribution_details
                                        "has_contributed_in_last_year": contrib_collection.get("hasAnyContributions"),
                                        **extracted_info
                                    }
                            
                            results[node_id] = {"data": latest_contribution_details}
                        else: 
                            # This part of your existing logic seems fine
                            context.log.warning(f"No data returned for node {node_id} in API response. Marking as inactive for this batch.")
                            results[node_id] = {"data": None}
                        processed_in_batch.add(node_id)
                    request_successful_for_batch = True 
                    break # Break from retry loop for the batch
            except requests.exceptions.HTTPError as e:
                context.log.warning(f"  HTTP error (Main Batch {i // batch_size + 1}, Attempt {attempt+1}): {e}")
                if e.response is not None:
                    context.log.warning(f"  Response status: {e.response.status_code}, Response text: {e.response.text[:500]}")
                    if e.response.status_code in (502, 503, 504): error_counts["count_502_errors"] +=1 # Grouping 50x errors
                    elif e.response.status_code in (403, 429): error_counts["count_403_errors"] += 1
                if attempt == max_retries_main_batch - 1: break # Failed all retries for the batch
                time.sleep((2 ** attempt) * 5 + random.uniform(0,1)); continue # Exponential backoff
            except requests.exceptions.RequestException as e: 
                context.log.warning(f"  RequestException (Main Batch {i // batch_size + 1}, Attempt {attempt+1}): {e}")
                if attempt == max_retries_main_batch - 1: break
                time.sleep((2 ** attempt) * 5 + random.uniform(0,1)); continue
            except Exception as e: 
                context.log.warning(f"  Unexpected error (Main Batch {i // batch_size + 1}, Attempt {attempt+1}): {e}")
                context.log.warning(traceback.format_exc())
                if attempt == max_retries_main_batch - 1: break
                time.sleep(5); continue

        if not request_successful_for_batch: 
            # Mark all nodes in this failed batch as having no data or needing retry
            for node_id_in_batch in current_batch_node_ids:
                if node_id_in_batch not in processed_in_batch:
                    results[node_id_in_batch] = {"is_active": False, "data": None} # Or some other failure indicator
            context.log.warning(f"Batch {i // batch_size + 1} failed after {max_retries_main_batch} attempts.")
            
        batch_processing_time = time.time() - batch_start_time
        batch_time_history.append(batch_processing_time)
        # Log average batch time less frequently or if changed significantly
        if len(batch_time_history) % 5 == 0 and len(batch_time_history) > 0 : 
            context.log.info(f"  Average total batch processing time over last {len(batch_time_history)} batches: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
        context.log.info(f"Batch {i // batch_size + 1} completed. Time: {batch_processing_time:.2f}s.")
        context.log.info("-" * 40)
    
    return results, error_counts


# Dagster Asset for Contributor Activity in past year (contributions only)
# to accommodate multiple environments, we will use a factory function
def create_latest_contributor_activity_asset(env_prefix: str):
    @dg.asset(
        key_prefix=env_prefix,
        name="latest_contributor_activity",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "github_api", "gcs"},
        group_name="ingestion",
        tags={"github_api": "True"},
        description="Retrieves GitHub user ID, login, and their latest activity (aiming for top 100) using the GitHub GraphQL API."
    )
    def _latest_contributor_activity_env_specific(context, config: GithubAssetConfig) -> dg.MaterializeResult:
        env_config = context.resources.active_env_config  
        raw_schema = env_config["raw_schema"]  
        clean_schema = env_config["clean_schema"] 

        # Access resources from the context object
        github_api = context.resources.github_api

        # tell the user what environment they are running in
        context.log.info(f"------************** Process is running in {env_config['env']} environment. *****************---------")

        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        gh_pat = github_api.get_client(config.key_name)

        if gh_pat is None:
            context.log.warning("No GitHub PAT found. Skipping asset.")
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})
        
        with cloud_sql_engine.connect() as conn:
            result = conn.execute(
                text(
                    f"""
                    SELECT DISTINCT 
                        lcd.contributor_node_id 
                    FROM {clean_schema}.latest_contributor_data lcd INNER JOIN {clean_schema}.latest_contributors lc 
                        ON lcd.contributor_unique_id_builder_love = lc.contributor_unique_id_builder_love
                    WHERE lcd.is_active = TRUE 
                    AND lower(lc.contributor_type) = 'user'
                    AND lcd.contributor_node_id IS NOT NULL
                    """
                )
            )
            distinct_contributor_node_ids_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if not isinstance(distinct_contributor_node_ids_df, pd.DataFrame) or distinct_contributor_node_ids_df.empty:
            context.log.error("No active contributor node IDs found in database.")
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})

        node_ids = distinct_contributor_node_ids_df['contributor_node_id'].tolist()
        context.log.info(f"Found {len(node_ids)} distinct contributor node IDs to process for recent activity.")

        # Call get_github_user_recent_contributions to get the data
        activity_results, count_http_errors = get_github_user_latest_activity(context, node_ids, gh_pat)

        if not activity_results:
            context.log.error("No activity results returned by get_github_user_latest_activity function.")
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})
        context.log.info(f"found {len(activity_results)} activity results. Proceeding to create dataframe...")

        processed_activity_rows = []
        for db_contributor_node_id, result_item in activity_results.items():
            if result_item.get("data") and isinstance(result_item.get("data"), dict):
                user_data = result_item["data"]
                if user_data.get("__typename") == "User":
                    github_api_user_node_id = user_data.get("id")
                    has_contributed_in_last_year = user_data.get("has_contributed_in_last_year")
                    row_data = {
                        "contributor_node_id": github_api_user_node_id,
                        "has_contributed_in_last_year": has_contributed_in_last_year,
                    }
                    processed_activity_rows.append(row_data)
        context.log.info(f"create flatted python list of activity rows, of length {len(processed_activity_rows)}. Proceeding to create dataframe...")

        if not processed_activity_rows:
            context.log.warning("No processed rows with activity to write to the database.")
            return dg.MaterializeResult(
                metadata={
                    "row_count": dg.MetadataValue.int(0),
                    "count_http_403_errors": dg.MetadataValue.int(count_http_errors['count_403_errors']),
                    "count_http_502_errors": dg.MetadataValue.int(count_http_errors['count_502_errors']),
                }
            )

        contributor_activity_df = pd.DataFrame(processed_activity_rows)
        context.log.info(f"Processed {len(contributor_activity_df)} contributors' activity into DataFrame.")
        contributor_activity_df['data_timestamp'] = pd.Timestamp.now()
        contributor_activity_df = contributor_node_id_swap(context, contributor_activity_df, cloud_sql_engine)

        if not isinstance(contributor_activity_df, pd.DataFrame) or contributor_activity_df.empty:
            context.log.error("no contributor results found after swapping legacy contributor node id for new format contributor node id")
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})
        
        target_table_name = 'latest_contributor_activity'
        target_schema = raw_schema
        try:
            with cloud_sql_engine.connect() as conn:
                with conn.begin():
                    context.log.info(f"writing to {target_schema}.{target_table_name} table. First truncating the table, if exists. Then appending the data, else creating the table.")
                    idempotent_truncate_sql = f"""
                    DO $$
                    BEGIN
                    IF EXISTS (SELECT FROM pg_catalog.pg_tables WHERE schemaname = '{target_schema}' AND tablename = '{target_table_name}') THEN
                        TRUNCATE TABLE {target_schema}.{target_table_name};
                    END IF;
                    END $$;
                    """
                    conn.execute(text(idempotent_truncate_sql))
                    contributor_activity_df.to_sql(
                        target_table_name, 
                        conn, 
                        if_exists='append', 
                        chunksize=50000, 
                        index=False, 
                        schema=target_schema,
                        method='multi' 
                    )
                    context.log.info(f"Table {target_schema}.{target_table_name} load successful.")
        except Exception as e:
            # fallback logic uses GCS
            context.log.error(f"Error writing to {target_schema}.{target_table_name} table: {e}. Writing to GCS as a fallback.")
            try:
                gcs_bucket_name = "bl-pipeline-backup-files"
                timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
                blob_name = f"latest_contributor_activity/fallback_{timestamp}.parquet"
                gcs_fallback_path = f"gs://{gcs_bucket_name}/{blob_name}"

                # Get the GCS client from resources
                gcs_resource = context.resources.gcs
                storage_client = gcs_resource.get_client()
                
                # Write the DataFrame to an in-memory buffer
                parquet_buffer = io.BytesIO()
                contributor_activity_df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0) # Rewind the buffer to the beginning

                # Upload the buffer to GCS
                bucket = storage_client.bucket(gcs_bucket_name)
                blob = bucket.blob(blob_name)
                blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')

                context.log.info(f"Fallback Parquet file saved to GCS: {gcs_fallback_path}")

            except Exception as pe:
                context.log.error(f"Error saving fallback file to GCS: {pe}")
            
            # Still return a failure result
            return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(0)})

        final_row_count = len(contributor_activity_df)
        preview_df = contributor_activity_df.head()

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(final_row_count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "count_http_403_errors": dg.MetadataValue.int(count_http_errors['count_403_errors']),
                "count_http_502_errors": dg.MetadataValue.int(count_http_errors['count_502_errors']),
            }
        )

    return _latest_contributor_activity_env_specific