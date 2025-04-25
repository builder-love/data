import dagster as dg
from dagster import asset
import os
import requests
import pandas as pd
import toml
from sqlalchemy import text
import sqlalchemy
import re
import time
import random
import gzip
import json
import psycopg2
from urllib.parse import urlparse

# function to get the most recently retrieved local copy of crypto ecosystems export.jsonl
def get_crypto_ecosystems_project_json(context):

    # use the dagster context variable from the calling function to get the local path to the cloned repo
    output_filepath = context.resources.electric_capital_ecosystems_repo['output_filepath']

    if not output_filepath:
        raise ValueError("output_filepath is empty")
    if not os.path.exists(output_filepath):
        raise ValueError("output_filepath does not exist")

    # read the exports.jsonl file
    try:
        df = pd.read_json(output_filepath, lines=True)

        # check if the dataframe is empty
        if df.empty:
            raise ValueError("DataFrame is empty")

        # rename the columns
        df.rename(columns={
            "eco_name": "project_title",
            "branch": "sub_ecosystems",
            "repo_url": "repo",
            "tags": "tags"
        }, inplace=True)

        # confirm the dataframe only has the columns we want
        expected_cols = [
            'project_title',
            'sub_ecosystems',
            'repo',
            'tags'
        ]

        # Get the actual columns from the DataFrame
        actual_cols = df.columns

        # Convert both to sets for easy comparison (ignores order)
        if set(expected_cols) != set(actual_cols):
            context.log.info(f"DataFrame columns: {df.columns}")
            context.log.info(f"Expected columns: {expected_cols}")
            raise ValueError("DataFrame columns are not as expected")

    except Exception as e:
        raise ValueError(f"Error reading exports.jsonl file: {e}")

    return df

# define the asset that gets the list of projects and associated repos from the local crypto ecosystems data file, exports.jsonl
@dg.asset(
    required_resource_keys={"electric_capital_ecosystems_repo", "cloud_sql_postgres_resource"},
    group_name="ingestion",
)
def crypto_ecosystems_project_json(context) -> dg.MaterializeResult:

    # get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the local path to the cloned repo
    output_filepath = context.resources.electric_capital_ecosystems_repo['output_filepath']

    if not output_filepath:
        raise ValueError("output_filepath is empty")
    if not os.path.exists(output_filepath):
        raise ValueError("output_filepath does not exist")

    # read the exports.jsonl file
    try:
        df = get_crypto_ecosystems_project_json(context)

        # add unix datetime column
        df['data_timestamp'] = pd.Timestamp.now()

        # here we truncate the existing table and append the new data
        # we do this to preserve the index 
        with cloud_sql_engine.connect() as conn:
            context.log.info("Truncating raw.crypto_ecosystems_raw_file_staging table")
            conn.execute(sqlalchemy.text("TRUNCATE TABLE raw.crypto_ecosystems_raw_file_staging;")) 
            conn.commit()

        context.log.info("Appending new data to raw.crypto_ecosystems_raw_file_staging table")
        df.to_sql('crypto_ecosystems_raw_file_staging', cloud_sql_engine, if_exists='append', index=False, schema='raw')

    except Exception as e:
        raise ValueError(f"Error reading exports.jsonl file: {e}")

    # capture asset metadata
    with cloud_sql_engine.connect() as conn:
        preview_query = text("select count(*) from raw.crypto_ecosystems_raw_file_staging")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from raw.crypto_ecosystems_raw_file_staging limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
            "unique_project_count": dg.MetadataValue.int(df['project_title'].nunique()),
            "unique_repo_count": dg.MetadataValue.int(df['repo'].nunique()),
        }
    )


# define the asset that gets the list of github orgs for a project
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
)
def github_project_orgs(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    query_text = """
        -- Specify the target table and the columns you want to insert into
        INSERT INTO raw.project_organizations (project_title, project_organization_url, data_timestamp)

        -- data to be inserted
        WITH projects AS (
        SELECT
            project_title,
            'https://github.com/' || split_part(repo, '/', 4) AS project_organization_url

        FROM raw.crypto_ecosystems_raw_file
        WHERE sub_ecosystems = '{}'
            AND split_part(repo, '/', 4) <> ''
        )

        SELECT DISTINCT ON (project_title) 
            project_title,             
            project_organization_url,  
            CURRENT_TIMESTAMP

        FROM projects
    """

    # send the insert into query to postgres 
    try: 
        with cloud_sql_engine.connect() as conn:
            conn.execute(text(query_text))
            conn.commit()

            # capture asset metadata
            preview_query = text("select count(*) from raw.project_organizations")
            result = conn.execute(preview_query)
            # Fetch all rows into a list of tuples
            row_count = result.fetchone()[0]

            preview_query = text("select * from raw.project_organizations limit 10")
            result = conn.execute(preview_query)
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

            return dg.MaterializeResult(
                metadata={
                    "raw_table_row_count": dg.MetadataValue.int(row_count),
                    "raw_table_preview": dg.MetadataValue.md(result_df.to_markdown(index=False))
                }
            )
    except Exception as e:
        raise ValueError(f"Error inserting into raw.project_organizations: {e}")


# define the asset that gets the active, distinct repo list from the latest_distinct_project_repos table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="clean_data",
    automation_condition=dg.AutomationCondition.eager(),
)
def latest_active_distinct_github_project_repos(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    def check_non_github_repo_exists(repo_url, repo_source):
        if repo_source == "bitbucket":
            # Extract owner and repo_slug from the URL
            try:
                parts = repo_url.rstrip('/').split('/')
                owner = parts[-2]
                repo_slug = parts[-1]
                if '.' in repo_slug:
                    repo_slug = repo_slug.split('.')[0]
            except IndexError:
                print(f"Invalid Bitbucket URL format: {repo_url}")
                return False

            # Construct the correct Bitbucket API endpoint
            api_url = f"https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}"

            response = requests.get(api_url)

            if response.status_code == 200:
                return True  # Repo exists and is accessible
            else:
                return False
        elif repo_source == "gitlab":
            try:
                parts = repo_url.rstrip('/').split('/')
                project_path = "/".join(parts[3:])
                project_path_encoded = requests.utils.quote(project_path, safe='')
            except IndexError:
                print(f"Invalid GitLab URL format: {repo_url}")
                return False

            api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

            try:
                response = requests.get(api_url)  # No headers needed for unauthenticated access
                print(f"Status Code: {response.status_code}, URL: {api_url}")

                if response.status_code == 200:
                    return True  # Repo exists and is public
                elif response.status_code == 404:
                    print(f"Repository not found (or private): {repo_url}")
                    return False
            except Exception as e:
                print(f"Error checking GitLab repo: {e}")
                return False
        else:
            return False

    def check_github_repo_exists(repo_urls, gh_pat, repo_source):
        """
        Checks if GitHub repository exists using the GraphQL API.

        Args:
            repo_urls: A list of GitHub repository URLs.

        Returns:
            A dictionary mapping each repository URL to True (exists and accessible)
            or False (doesn't exist or not accessible).
        """

        if not repo_urls:  # Handle empty input list
            return [], 0

        api_url = "https://api.github.com/graphql"
        headers = {"Authorization": f"bearer {gh_pat}"}
        results = {}  # Store results: {url: True/False}
        batch_size = 200  # Adjust as needed
        cpu_time_used = 0
        real_time_used = 0
        real_time_window = 60
        cpu_time_limit = 50
        count_403_errors = 0
        count_502_errors = 0
        batch_time_history = []

        for i in range(0, len(repo_urls), batch_size):
            print(f"processing batch: {i} - {i + batch_size}")
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
                    print(f"Invalid GitHub URL format: {repo_url}")
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
                    isPrivate
                }}\n"""

            query += "}"

            base_delay = 1
            max_delay = 60
            max_retries = 8

            for attempt in range(max_retries):
                print(f"attempt: {attempt}")
                
                try:
                    if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                        extra_delay = (cpu_time_used - cpu_time_limit) / 2
                        extra_delay = max(1, extra_delay)
                        print(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                        time.sleep(extra_delay)
                        print(f"resetting cpu_time_used and real_time_used to 0")
                        cpu_time_used = 0
                        real_time_used = 0
                        start_time = time.time()
                    elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                        print(f"real time limit reached without CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                        print(f"real time limit reached. CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                        print('cpu time limit not reached. Continuing...')
                    
                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)

                    time_since_start = time.time() - start_time
                    print(f"time_since_start: {time_since_start:.2f} seconds")
                    time.sleep(2.5)  # Consistent delay
                    
                    # use raise for status to catch errors
                    response.raise_for_status()
                    data = response.json()

                    if 'errors' in data:

                        print(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        print(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        print(f"Rate Limit Info: {rate_limit_info}\n")

                        for error in data['errors']:
                            if error['type'] == 'RATE_LIMITED':
                                reset_at = response.headers.get('X-RateLimit-Reset')
                                if reset_at:
                                    delay = int(reset_at) - int(time.time()) + 1
                                    delay = max(1, delay)
                                    delay = min(delay, max_delay)
                                    print(f"Rate limited.  Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue  # Retry the entire batch
                            else:
                                print(f"GraphQL Error: {error}") #Print all the errors.

                    if 'data' in data:
                        for j, repo_url in enumerate(batch):
                            if repo_url in processed_in_batch:  # CRUCIAL CHECK
                                continue  # Skip if already processed
                            
                            repo_data = data['data'].get(f'repo{j}')
                            
                            # if repo isPrivate is true, print the repo url
                            if repo_data and repo_data['isPrivate']:
                                print(f"repo is private: {repo_url}")

                            if repo_data and repo_data['isPrivate'] == False:
                                results[repo_url] = {"is_active": True, "repo_source": repo_source}
                                processed_in_batch.add(repo_url)  # Mark as processed
                            else:
                                print(f"repo_data is empty for repo: {repo_url}\n")
                                # don't return here, return errors at end of batch
                    break

                except requests.exceptions.RequestException as e:
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print(f"Status Code: {response.status_code}")

                    # Extract rate limit information from headers
                    print(" \nresource usage tracking:")
                    rate_limit_info = {
                        'remaining': response.headers.get('x-ratelimit-remaining'),
                        'used': response.headers.get('x-ratelimit-used'),
                        'reset': response.headers.get('x-ratelimit-reset'),
                        'retry_after': response.headers.get('retry-after')
                    }
                    print(f"Rate Limit Info: {rate_limit_info}\n")

                    print(f"the error is: {e}\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up.")
                        # don't return here, return errors at end of batch
                        break
                    
                    # rate limit handling
                    if isinstance(e, requests.exceptions.HTTPError):
                        if e.response.status_code in (502, 504):
                            count_502_errors +=1
                            print(f"This process has generated {count_502_errors} 502/504 errors in total.")
                            delay = 1
                            print(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)
                            continue
                        elif e.response.status_code in (403, 429):
                            count_403_errors += 1
                            print(f"This process has generated {count_403_errors} 403/429 errors in total.")
                            retry_after = e.response.headers.get('Retry-After')
                            if retry_after:
                                delay = int(retry_after)
                                print(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                time.sleep(delay)
                                continue
                            else:
                                delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                print(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                    else:
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                        time.sleep(delay)

                except KeyError as e:
                    print(f"KeyError: {e}. Response: {data}")
                    # Don't append here; handle errors at the end
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    # Don't append here; handle errors at the end
                    break

            # Handle any repos that failed *all* retries (or were invalid URLs)
            for repo_url in batch:
                if repo_url not in processed_in_batch:
                    print(f"adding repo to results after max retries, or was invalid url: {repo_url}")
                    results[repo_url] = {"is_active": False, "repo_source": repo_source}

            # calculate the time it takes to process the batch
            end_time = time.time()
            batch_time = end_time - start_time
            cpu_time_used += time_since_start
            real_time_used += batch_time
            batch_time_history.append(batch_time)
            if batch_time_history and len(batch_time_history) > 10:
                print(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
            print(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
            print(f"time taken to process batch {i}: {batch_time:.2f} seconds")
            print(f"Total CPU time used: {cpu_time_used:.2f} seconds")
            print(f"Total real time used: {real_time_used:.2f} seconds")

        return results, {"count_403_errors": count_403_errors, "count_502_errors": count_502_errors}

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("""select repo, repo_source from clean.latest_distinct_project_repos""")
            )
        distinct_repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = distinct_repo_df[distinct_repo_df['repo_source'] == 'github']['repo'].tolist()

    # get github pat
    gh_pat = os.getenv('go_blockchain_ecosystem')

    github_results = check_github_repo_exists(github_urls, gh_pat, 'github')

    results = github_results[0]
    count_http_errors = github_results[1]

    # write results to pandas dataframe
    results_df = pd.DataFrame(
        [
            {"repo": url, "is_active": data["is_active"], "repo_source": data["repo_source"]}
            for url, data in results.items()
        ]
    )

    # now get non-github repos urls
    non_github_urls = distinct_repo_df[distinct_repo_df['repo_source'] != 'github']

    # if non_github_urls is not empty, apply check_non_github_repo_exists
    if not non_github_urls.empty:
        print("found non-github repos. Getting active status...")
        # apply distinct_repo_df['repo'] to check_repo_exists
        non_github_urls['is_active'] = non_github_urls.apply(
            lambda row: check_non_github_repo_exists(row['repo'], row['repo_source']), axis=1
        )

        # append non_github_urls to results_df
        results_df = pd.concat([results_df, non_github_urls])

    # add unix datetime column
    results_df['data_timestamp'] = pd.Timestamp.now()

    # write the data to the latest_active_distinct_project_repos table
    results_df.to_sql('latest_active_distinct_project_repos', cloud_sql_engine, if_exists='replace', index=False, schema='raw')

    with cloud_sql_engine.connect() as conn:

        # # capture asset metadata
        preview_query = text("select count(*) from raw.latest_active_distinct_project_repos")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from raw.latest_active_distinct_project_repos")
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

# define the asset that gets the stargaze count for a repo
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_repos_stargaze_count(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_non_github_repo_stargaze_count(repo_url, repo_source):

        print(f"processing non-githubrepo: {repo_url}")

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
                print(f"Invalid Bitbucket URL format: {repo_url}")
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
                print(f"Error fetching data from Bitbucket API: {e}")
                return None
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}")
                return None
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return None

        elif repo_source == "gitlab":
            try:
                parts = repo_url.rstrip('/').split('/')
                project_path = "/".join(parts[3:])
                project_path_encoded = requests.utils.quote(project_path, safe='')
            except IndexError:
                print(f"Invalid GitLab URL format: {repo_url}")
                return None

            api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

            try:
                response = requests.get(api_url)  # No headers needed for unauthenticated access
                response.raise_for_status()

                # return the stargaze count
                return response.json()['star_count']
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from GitLab API: {e}")
                return None
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}") 
                return None
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
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
            print(f"processing batch: {i} - {i + batch_size}")
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
                    print(f"Invalid GitHub URL format: {repo_url}")
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
                print(f"attempt: {attempt}")
                
                try:
                    if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                        extra_delay = (cpu_time_used - cpu_time_limit) / 2
                        extra_delay = max(1, extra_delay)
                        print(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                        time.sleep(extra_delay)
                        print(f"resetting cpu_time_used and real_time_used to 0")
                        cpu_time_used = 0
                        real_time_used = 0
                        start_time = time.time()
                    elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                        print(f"real time limit reached without CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                        print(f"real time limit reached. CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                        print('cpu time limit not reached. Continuing...')

                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)

                    time_since_start = time.time() - start_time
                    print(f"time_since_start: {time_since_start:.2f} seconds")
                    time.sleep(3)  # Consistent delay
                    
                    # use raise for status to catch errors
                    response.raise_for_status()
                    data = response.json()

                    if 'errors' in data:
                        print(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        print(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        print(f"Rate Limit Info: {rate_limit_info}\n")

                        for error in data['errors']:
                            if error['type'] == 'RATE_LIMITED':
                                reset_at = response.headers.get('X-RateLimit-Reset')
                                if reset_at:
                                    delay = int(reset_at) - int(time.time()) + 1
                                    delay = max(1, delay)
                                    delay = min(delay, max_delay)
                                    print(f"Rate limited.  Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue  # Retry the entire batch
                            else:
                                print(f"GraphQL Error: {error}") #Print all the errors.

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
                                print(f"repo_data is empty for repo: {repo_url}\n")
                                # don't return here, return errors at end of batch
                    break

                except requests.exceptions.RequestException as e:
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print(f"Status Code: {response.status_code}")

                    # Extract rate limit information from headers
                    print(" \n resource usage tracking:")
                    rate_limit_info = {
                        'remaining': response.headers.get('x-ratelimit-remaining'),
                        'used': response.headers.get('x-ratelimit-used'),
                        'reset': response.headers.get('x-ratelimit-reset'),
                        'retry_after': response.headers.get('retry-after')
                    }
                    print(f"Rate Limit Info: {rate_limit_info}\n")

                    print(f"the error is: {e}\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up.")
                        # don't return here, return errors at end of batch
                        break

                    # rate limit handling
                    if isinstance(e, requests.exceptions.HTTPError):
                        if e.response.status_code in (502, 504):
                            count_502_errors += 1
                            print(f"This process has generated {count_502_errors} 502/504 errors in total.")
                            delay = 1
                            print(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)
                            continue
                        elif e.response.status_code in (403, 429):
                            count_403_errors += 1
                            print(f"This process has generated {count_403_errors} 403/429 errors in total.")
                            retry_after = e.response.headers.get('Retry-After')
                            if retry_after:
                                delay = int(retry_after)
                                print(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                time.sleep(delay)
                                continue
                            else:
                                delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                print(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                    else:
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                        time.sleep(delay)

                except KeyError as e:
                    print(f"KeyError: {e}. Response: {data}")
                    # Don't append here; handle errors at the end
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    # Don't append here; handle errors at the end
                    break

            # Handle any repos that failed *all* retries (or were invalid URLs)
            for repo_url in batch:
                if repo_url not in processed_in_batch:
                    results[repo_url] = None
                    print(f"adding repo to results after max retries, or was invalid url: {repo_url}")

            # calculate the time it takes to process the batch
            end_time = time.time()
            batch_time = end_time - start_time
            cpu_time_used += time_since_start
            real_time_used += batch_time
            batch_time_history.append(batch_time)
            if batch_time_history and len(batch_time_history) > 10:
                print(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
            print(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
            print(f"time taken to process batch {i}: {batch_time:.2f} seconds")
            print(f"Total CPU time used: {cpu_time_used:.2f} seconds")
            print(f"Total real time used: {real_time_used:.2f} seconds")

        return results, {
            'count_403_errors': count_403_errors,
            'count_502_errors': count_502_errors
        }

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("""select repo, repo_source from clean.latest_active_distinct_project_repos where is_active = true"""
                )
            )
        repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

    # get github pat
    gh_pat = os.getenv('go_blockchain_ecosystem')

    results = get_github_repo_stargaze_count(github_urls, gh_pat)

    github_results = results[0]
    count_http_errors_github_api = results[1]

    # write results to pandas dataframe
    results_df = pd.DataFrame(github_results.items(), columns=['repo', 'stargaze_count'])

    # now get non-github repos urls
    non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

    # if non_github_urls is not empty, get stargaze count
    if not non_github_results_df.empty:
        print("found non-github repos. Getting repo stargaze count...")
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
    results_df.to_sql('project_repos_stargaze_count', cloud_sql_engine, if_exists='append', index=False, schema='raw')

    with cloud_sql_engine.connect() as conn:
        # capture asset metadata
        preview_query = text("select count(*) from raw.project_repos_stargaze_count")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from raw.project_repos_stargaze_count limit 10")
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

# define the asset that gets the fork count for a repo
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_repos_fork_count(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_non_github_repo_fork_count(repo_url, repo_source):

        print(f"processing non-githubrepo: {repo_url}")

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
                print(f"Invalid Bitbucket URL format: {repo_url}")
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
                print(f"Error fetching data from Bitbucket API: {e}")
                return None
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}")
                return None
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return None

        elif repo_source == "gitlab":
            try:
                parts = repo_url.rstrip('/').split('/')
                project_path = "/".join(parts[3:])
                project_path_encoded = requests.utils.quote(project_path, safe='')
            except IndexError:
                print(f"Invalid GitLab URL format: {repo_url}")
                return None

            api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

            try:
                response = requests.get(api_url)  # No headers needed for unauthenticated access
                response.raise_for_status()

                # return the fork count
                return response.json()['forks_count']
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from GitLab API: {e}")
                return None
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}") 
                return None
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
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
            print(f"processing batch: {i} - {i + batch_size}")
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
                    print(f"Invalid GitHub URL format: {repo_url}")
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
                print(f"attempt: {attempt}")
                
                try:
                    if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                        extra_delay = (cpu_time_used - cpu_time_limit) / 2
                        extra_delay = max(1, extra_delay)
                        print(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                        time.sleep(extra_delay)
                        print(f"resetting cpu_time_used and real_time_used to 0")
                        cpu_time_used = 0
                        real_time_used = 0
                        start_time = time.time()
                    elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                        print(f"real time limit reached without CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                        print(f"real time limit reached. CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                        print('cpu time limit not reached. Continuing...')

                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                    time_since_start = time.time() - start_time
                    print(f"time_since_start: {time_since_start:.2f} seconds")
                    time.sleep(3)  # Consistent delay

                    # use raise for status to catch errors
                    response.raise_for_status()
                    data = response.json()

                    if 'errors' in data:
                        print(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        print(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        print(f"Rate Limit Info: {rate_limit_info}\n")

                        for error in data['errors']:
                            if error['type'] == 'RATE_LIMITED':
                                reset_at = response.headers.get('X-RateLimit-Reset')
                                if reset_at:
                                    delay = int(reset_at) - int(time.time()) + 1
                                    delay = max(1, delay)
                                    delay = min(delay, max_delay)
                                    print(f"Rate limited.  Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue  # Retry the entire batch
                            else:
                                print(f"GraphQL Error: {error}") #Print all the errors.

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
                                print(f"repo_data is empty for repo: {repo_url}\n")
                    break

                except requests.exceptions.RequestException as e:
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print(f"Status Code: {response.status_code}")
                    # Extract rate limit information from headers
                    print(" \n resource usage tracking:")
                    rate_limit_info = {
                        'remaining': response.headers.get('x-ratelimit-remaining'),
                        'used': response.headers.get('x-ratelimit-used'),
                        'reset': response.headers.get('x-ratelimit-reset'),
                        'retry_after': response.headers.get('retry-after')
                    }
                    print(f"Rate Limit Info: {rate_limit_info}\n")

                    print(f"the error is: {e}\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up.")
                        break
                    # --- Rate Limit Handling (REST API style - for 403/429) ---
                    if isinstance(e, requests.exceptions.HTTPError):
                        if e.response.status_code in (502, 504):
                            count_502_errors += 1
                            print(f"This process has generated {count_502_errors} 502/504 errors in total.")
                            delay = 1
                            print(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)
                            continue
                        elif e.response.status_code in (403, 429):
                            count_403_errors += 1
                            print(f"This process has generated {count_403_errors} 403/429 errors in total.")
                            retry_after = response.headers.get('Retry-After')
                            if retry_after:
                                delay = int(retry_after)
                                print(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                time.sleep(delay)
                                continue
                            else:
                                delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                print(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                    else:
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                        time.sleep(delay)

                except KeyError as e:
                    print(f"KeyError: {e}. Response: {data}")
                    # Don't append here; handle errors at the end
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    # Don't append here; handle errors at the end
                    break

            # Handle any repos that failed *all* retries (or were invalid URLs)
            for repo_url in batch:
                if repo_url not in processed_in_batch:
                    results[repo_url] = None
                    print(f"adding repo to results after max retries, or was invalid url: {repo_url}")

            end_time = time.time()
            batch_time = end_time - start_time
            cpu_time_used += time_since_start
            real_time_used += batch_time
            batch_time_history.append(batch_time)
            if batch_time_history and len(batch_time_history) > 10:
                print(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
            print(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
            print(f"time taken to process batch {i}: {batch_time:.2f} seconds")
            print(f"Total CPU time used: {cpu_time_used:.2f} seconds")
            print(f"Total real time used: {real_time_used:.2f} seconds")

        return results, {
            'count_403_errors': count_403_errors,
            'count_502_errors': count_502_errors
        }

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("""select repo, repo_source from clean.latest_active_distinct_project_repos where is_active = true"""
                )
            )
        repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

    print(f"number of github urls: {len(github_urls)}")

    results = get_github_repo_fork_count(github_urls, gh_pat)

    github_results = results[0]
    count_http_errors_github_api = results[1]

    # write results to pandas dataframe
    results_df = pd.DataFrame(github_results.items(), columns=['repo', 'fork_count'])

    # now get non-github repos urls
    non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

    # if non_github_urls is not empty, get fork count
    if not non_github_results_df.empty:
        print("found non-github repos. Getting repo fork count...")
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
    results_df.to_sql('project_repos_fork_count', cloud_sql_engine, if_exists='append', index=False, schema='raw')

    with cloud_sql_engine.connect() as conn:
        # capture asset metadata
        preview_query = text("select count(*) from raw.project_repos_fork_count")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from raw.project_repos_fork_count limit 10")
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


@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_repos_languages(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_non_github_repo_languages(repo_url, repo_source):

        print(f"processing non-githubrepo: {repo_url}")

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
                print(f"Invalid Bitbucket URL format: {repo_url}")
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
                print(f"Error fetching data from Bitbucket API: {e}")
                return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}")
                return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}

        elif repo_source == "gitlab":
            try:
                parts = repo_url.rstrip('/').split('/')
                project_path = "/".join(parts[3:])
                project_path_encoded = requests.utils.quote(project_path, safe='')
            except IndexError:
                print(f"Invalid GitLab URL format: {repo_url}")
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
                print(f"Error fetching data from GitLab API: {e}")
                return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}") 
                return {'repo': repo_url, 'language_name': None, 'size': None, 'repo_languages_total_bytes': None}
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
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
            print(f"processing batch: {i} - {i + batch_size}")
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
                    print(f"Invalid GitHub URL format: {repo_url}")
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
                print(f"attempt: {attempt}")

                try:
                    if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                        extra_delay = (cpu_time_used - cpu_time_limit) / 2
                        extra_delay = max(1, extra_delay)
                        print(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                        time.sleep(extra_delay)
                        print(f"resetting cpu_time_used and real_time_used to 0")
                        cpu_time_used = 0
                        real_time_used = 0
                        start_time = time.time()
                    elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                        print(f"real time limit reached without CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                        print(f"real time limit reached. CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                        print('cpu time limit not reached. Continuing...')

                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                    time_since_start = time.time() - start_time
                    print(f"time_since_start: {time_since_start:.2f} seconds")
                    time.sleep(2.5)  # Consistent delay

                    response.raise_for_status()
                    data = response.json()

                    if 'errors' in data:
                        print(f"Status Code: {response.status_code}")
                        print(" \nresource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        print(f"Rate Limit Info: {rate_limit_info}\n")

                        for error in data['errors']:
                            if error['type'] == 'RATE_LIMITED':
                                reset_at = response.headers.get('X-RateLimit-Reset')
                                if reset_at:
                                    delay = int(reset_at) - int(time.time()) + 1
                                    delay = max(1, delay)
                                    delay = min(delay, max_delay)
                                    print(f"Rate limited.  Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue  # Retry the entire batch

                                else:
                                    print(f"GraphQL Error: {error}")  # Print all the errors.

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
                                print(f"repo_data is empty for repo: {repo_url}\n")
                                # Don't append here; handle missing data at the end
                    break  # Exit retry loop if successful

                except requests.exceptions.RequestException as e:
                # ... (Your existing exception handling - remains largely unchanged) ...
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print(f"Status Code: {response.status_code}")
                    # Extract rate limit information from headers
                    print(" \nresource usage tracking:")
                    rate_limit_info = {
                        'remaining': response.headers.get('x-ratelimit-remaining'),
                        'used': response.headers.get('x-ratelimit-used'),
                        'reset': response.headers.get('x-ratelimit-reset'),
                        'retry_after': response.headers.get('retry-after')
                    }
                    print(f"Rate Limit Info: {rate_limit_info}\n")

                    print(f"the error is: {e}\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up.")
                        # Don't append here; handle failures at the end
                        break

                    if isinstance(e, requests.exceptions.HTTPError):
                        if e.response.status_code in (502, 504):
                            count_502_errors += 1
                            print(f"This process has generated {count_502_errors} 502/504 errors in total.")
                            delay = 1
                            print(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)
                            continue
                        elif e.response.status_code in (403, 429):
                            count_403_errors += 1
                            print(f"This process has generated {count_403_errors} 403/429 errors in total.")
                            retry_after = response.headers.get('Retry-After')
                            if retry_after:
                                delay = int(retry_after)
                                print(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                time.sleep(delay)
                                continue
                            else:
                                delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                print(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                    else:
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                        time.sleep(delay)

                except KeyError as e:
                    print(f"KeyError: {e}. Response: {data}")
                    # Don't append here; handle errors at the end
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    # Don't append here; handle errors at the end
                    break

            # Handle any repos that failed *all* retries (or were invalid URLs)
            for repo_url in batch:
                if repo_url not in processed_in_batch:
                    results.append({'repo': repo_url, 'languages_data': None, 'total_size': None})
                    print(f"adding repo to results after max retries, or was invalid url: {repo_url}")

            end_time = time.time()
            batch_time = end_time - start_time
            cpu_time_used += time_since_start
            real_time_used += batch_time
            batch_time_history.append(batch_time)
            if batch_time_history and len(batch_time_history) > 10:
                print(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
            print(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
            print(f"time taken to process batch {i}: {batch_time:.2f} seconds")
            print(f"Total CPU time used: {cpu_time_used:.2f} seconds")
            print(f"Total real time used: {real_time_used:.2f} seconds")

        return results, {"count_403_errors": count_403_errors, "count_502_errors": count_502_errors}

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("select repo, repo_source from clean.latest_active_distinct_project_repos where is_active = true")
        )
        repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

    print(f"number of github urls: {len(github_urls)}")

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
                print(f"languages_list is empty for repo: {repo_url}")
                data.append((repo_url, None, None, repo_languages_total_bytes))
        return data

    # ########################## github_repo_languages
    # check if results_df is not empty
    if not results_df.empty:
        new_rows = results_df[['repo', 'languages_data', 'total_size']].apply(lambda row: unpack_list(row, 'languages_data'), axis=1).explode()

        # Create a new DataFrame from the unpacked data
        unpacked_df = pd.DataFrame(new_rows.tolist(), columns=['repo', 'language_name', 'size', 'repo_languages_total_bytes'])

        # print column names
        print("unpacked_df column names:")
        print(unpacked_df.columns)

    # now get non-github repos urls
    non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

    # if non_github_urls is not empty, get language data
    if not non_github_results_df.empty:
        print("found non-github repos. Getting repo language data...")
        # Use list comprehension to get data for each non-github repo
        language_data = [
            get_non_github_repo_languages(row['repo'], row['repo_source'])
            for _, row in non_github_results_df.iterrows()  # Or use itertuples for slight improvement
        ]

        # create a df
        non_github_results_df = pd.DataFrame(language_data)

        # print column names
        print("non_github_results_df column names:")
        print(non_github_results_df.columns)

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
                schema='raw',
                dtype={
                    "size": sqlalchemy.types.BIGINT,
                    "repo_languages_total_bytes": sqlalchemy.types.BIGINT
                    }
                )

            # create variable to store the count of rows written to the database
            row_count_this_run = all_repos_df.shape[0]

            with cloud_sql_engine.connect() as conn:
                # capture asset metadata
                preview_query = text("select count(*) from raw.project_repos_languages")
                result = conn.execute(preview_query)
                # Fetch all rows into a list of tuples
                row_count = result.fetchone()[0]

                preview_query = text("select * from raw.project_repos_languages limit 10")
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
        print(f"error: {e}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
            "row_count_this_run": dg.MetadataValue.int(row_count_this_run),
            "count_http_403_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_403_errors']),
            "count_http_502_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_502_errors']),
        }
    )


@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_repos_commits(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_non_github_repo_commits(repo_url, repo_source):

        print(f"processing non-githubrepo: {repo_url}")

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
                print(f"Invalid Bitbucket URL format: {repo_url}")
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
                        print(f"Error: {response.status_code}")
                        break  # Exit the loop on error

                # Get the commit count from the 'size' field
                return {'repo': repo_url, 'commit_count': commit_count}

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from Bitbucket API: {e}")
                return {'repo': repo_url, 'commit_count': None}
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}")
                return {'repo': repo_url, 'commit_count': None}
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return {'repo': repo_url, 'commit_count': None}

        elif repo_source == "gitlab":
            try:
                parts = repo_url.rstrip('/').split('/')
                project_path = "/".join(parts[3:])
                project_path_encoded = requests.utils.quote(project_path, safe='')
            except IndexError:
                print(f"Invalid GitLab URL format: {repo_url}")
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
                            print(f"Error: {response.status_code}")
                            break
                    except requests.exceptions.RequestException as e:
                        print(f"Error fetching data from GitLab API: {e}")
                        break
                
                # return the commit count data
                if commits:
                    return {'repo': repo_url, 'commit_count': len(commits)}
                else:
                    return {'repo': repo_url, 'commit_count': None}
                    
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from GitLab API: {e}")
                return {'repo': repo_url, 'commit_count': None}
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}") 
                return {'repo': repo_url, 'commit_count': None}
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
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
            print(f"processing batch: {i} - {i + batch_size}")
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
                    print(f"Invalid GitHub URL format: {repo_url}")
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
                print(f"attempt: {attempt}")

                try:
                    if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                        extra_delay = (cpu_time_used - cpu_time_limit) / 2
                        extra_delay = max(1, extra_delay)
                        print(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                        time.sleep(extra_delay)
                        print(f"resetting cpu_time_used and real_time_used to 0")
                        cpu_time_used = 0
                        real_time_used = 0
                        start_time = time.time()
                    elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                        print(f"real time limit reached without CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                        print(f"real time limit reached. CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                        print('cpu time limit not reached. Continuing...')

                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                    # # Calculate the size of the headers
                    # header_size = len(str(response.headers).encode('utf-8'))
                    # print(f"Response Header Size: {header_size} bytes")
                    # print("Response Headers:")
                    # for key, value in response.headers.items():
                    #     print(f"{key}: {value}")
                    time_since_start = time.time() - start_time
                    print(f"time_since_start: {time_since_start:.2f} seconds")
                    time.sleep(1.2)  # Consistent delay

                    response.raise_for_status()
                    data = response.json()

                    if 'errors' in data:
                        print(f"Status Code: {response.status_code}")
                        print(" \nresource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        print(f"Rate Limit Info: {rate_limit_info}\n")

                        for error in data['errors']:
                            if error['type'] == 'RATE_LIMITED':
                                reset_at = response.headers.get('X-RateLimit-Reset')
                                if reset_at:
                                    delay = int(reset_at) - int(time.time()) + 1
                                    delay = max(1, delay)
                                    delay = min(delay, max_delay)
                                    print(f"Rate limited.  Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue  # Retry the entire batch

                                else:
                                    print(f"GraphQL Error: {error}")  # Print all the errors.

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
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print(f"Status Code: {response.status_code}")
                    # Extract rate limit information from headers
                    print(" \nresource usage tracking:")
                    rate_limit_info = {
                        'remaining': response.headers.get('x-ratelimit-remaining'),
                        'used': response.headers.get('x-ratelimit-used'),
                        'reset': response.headers.get('x-ratelimit-reset'),
                        'retry_after': response.headers.get('retry-after')
                    }
                    print(f"Rate Limit Info: {rate_limit_info}\n")

                    print(f"the error is: {e}\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up.")
                        # Don't append here; handle failures at the end
                        break

                    if isinstance(e, requests.exceptions.HTTPError):
                        if e.response.status_code in (502, 504):
                            count_502_errors += 1
                            print(f"This process has generated {count_502_errors} 502/504 errors in total.")
                            delay = 1
                            print(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)
                            continue
                        elif e.response.status_code in (403, 429):
                            count_403_errors += 1
                            print(f"This process has generated {count_403_errors} 403/429 errors in total.")
                            retry_after = response.headers.get('Retry-After')
                            if retry_after:
                                delay = int(retry_after)
                                print(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                time.sleep(delay)
                                continue
                            else:
                                delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                print(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                    else:
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                        time.sleep(delay)

                except KeyError as e:
                    print(f"KeyError: {e}. Response: {data}")
                    # Don't append here; handle errors at the end
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    # Don't append here; handle errors at the end
                    break

            # Handle any repos that failed *all* retries (or were invalid URLs)
            for repo_url in batch:
                if repo_url not in processed_in_batch:
                    results[repo_url] = None
                    print(f"adding repo to results after max retries, or was invalid url: {repo_url}")

            end_time = time.time()
            batch_time = end_time - start_time
            cpu_time_used += time_since_start
            real_time_used += batch_time
            batch_time_history.append(batch_time)
            if batch_time_history and len(batch_time_history) > 10:
                print(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
            print(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
            print(f"time taken to process batch {i}: {batch_time:.2f} seconds")
            print(f"Total CPU time used: {cpu_time_used:.2f} seconds")
            print(f"Total real time used: {real_time_used:.2f} seconds")

        return results, {"count_403_errors": count_403_errors, "count_502_errors": count_502_errors}

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("select repo, repo_source from clean.latest_active_distinct_project_repos where is_active = true")
        )
        repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()
    # filter for non-github repos urls
    non_github_results_df = repo_df[repo_df['repo_source'] != 'github']

    if len(github_urls) > 0:
        print(f"number of github urls: {len(github_urls)}")
        
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
        print("found non-github repos. Getting repo commit data...")
        print(f"number of non-github urls: {non_github_results_df.shape[0]}")
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
            all_repos_df.to_sql('project_repos_commit_count', cloud_sql_engine, if_exists='append', index=False, schema='raw')

            # create variable to store the count of rows written to the database
            row_count_this_run = all_repos_df.shape[0]
        else:
            # raise an error
            raise ValueError("No data to write")
            row_count_this_run = 0

        with cloud_sql_engine.connect() as conn:
            # capture asset metadata
            preview_query = text("select count(*) from raw.project_repos_commit_count")
            result = conn.execute(preview_query)
            # Fetch all rows into a list of tuples
            row_count = result.fetchone()[0]

            preview_query = text("select * from raw.project_repos_commit_count limit 10")
            result = conn.execute(preview_query)
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as e:
        print(f"error: {e}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
            "row_count_this_run": dg.MetadataValue.int(row_count_this_run),
            "count_http_403_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_403_errors']),
            "count_http_502_errors_github_api": dg.MetadataValue.int(count_http_errors_github_api['count_502_errors']),
        }
    )


# define the asset that gets the watcher count for a repo
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_repos_watcher_count(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_non_github_repo_watcher_count(repo_url, repo_source):

        print(f"processing non-githubrepo: {repo_url}")

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
                print(f"Invalid Bitbucket URL format: {repo_url}")
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
                print(f"Error fetching data from Bitbucket API: {e}")
                return None
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}")
                return None
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return None

        elif repo_source == "gitlab":
            try:
                parts = repo_url.rstrip('/').split('/')
                project_path = "/".join(parts[3:])
                project_path_encoded = requests.utils.quote(project_path, safe='')
            except IndexError:
                print(f"Invalid GitLab URL format: {repo_url}")
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
                            print(f"Error: {response.status_code}")
                            break
                    except requests.exceptions.RequestException as e:
                        print(f"Error fetching data from GitLab API: {e}")
                        break
                
                # return the watcher count data
                if watchers:
                    return len(watchers)
                else:
                    return None
                    
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from GitLab API: {e}")
                return None
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}") 
                return None
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
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
            print(f"processing batch: {i} - {i + batch_size}")
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
                    print(f"Invalid GitHub URL format: {repo_url}")
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
                print(f"attempt: {attempt}")
                
                try:
                    if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                        extra_delay = (cpu_time_used - cpu_time_limit) / 2
                        extra_delay = max(1, extra_delay)
                        print(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                        time.sleep(extra_delay)
                        print(f"resetting cpu_time_used and real_time_used to 0")
                        cpu_time_used = 0
                        real_time_used = 0
                        start_time = time.time()
                    elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                        print(f"real time limit reached without CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                        print(f"real time limit reached. CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                        print('cpu time limit not reached. Continuing...')

                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                    time_since_start = time.time() - start_time
                    print(f"time_since_start: {time_since_start:.2f} seconds")
                    time.sleep(2)  # Consistent delay

                    # use raise for status to catch errors
                    response.raise_for_status()
                    data = response.json()

                    if 'errors' in data:
                        print(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        print(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        print(f"Rate Limit Info: {rate_limit_info}\n")

                        for error in data['errors']:
                            if error['type'] == 'RATE_LIMITED':
                                reset_at = response.headers.get('X-RateLimit-Reset')
                                if reset_at:
                                    delay = int(reset_at) - int(time.time()) + 1
                                    delay = max(1, delay)
                                    delay = min(delay, max_delay)
                                    print(f"Rate limited.  Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue  # Retry the entire batch
                            else:
                                print(f"GraphQL Error: {error}") #Print all the errors.

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
                                print(f"repo_data is empty for repo: {repo_url}\n")
                    break

                except requests.exceptions.RequestException as e:
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print(f"Status Code: {response.status_code}")
                    # Extract rate limit information from headers
                    print(" \n resource usage tracking:")
                    rate_limit_info = {
                        'remaining': response.headers.get('x-ratelimit-remaining'),
                        'used': response.headers.get('x-ratelimit-used'),
                        'reset': response.headers.get('x-ratelimit-reset'),
                        'retry_after': response.headers.get('retry-after')
                    }
                    print(f"Rate Limit Info: {rate_limit_info}\n")

                    print(f"the error is: {e}\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up.")
                        break
                    # --- Rate Limit Handling (REST API style - for 403/429) ---
                    if isinstance(e, requests.exceptions.HTTPError):
                        if e.response.status_code in (502, 504):
                            count_502_errors += 1
                            print(f"This process has generated {count_502_errors} 502/504 errors in total.")
                            delay = 1
                            print(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)
                            continue
                        elif e.response.status_code in (403, 429):
                            count_403_errors += 1
                            print(f"This process has generated {count_403_errors} 403/429 errors in total.")
                            retry_after = response.headers.get('Retry-After')
                            if retry_after:
                                delay = int(retry_after)
                                print(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                time.sleep(delay)
                                continue
                            else:
                                delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                print(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                    else:
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                        time.sleep(delay)

                except KeyError as e:
                    print(f"KeyError: {e}. Response: {data}")
                    # Don't append here; handle errors at the end
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    # Don't append here; handle errors at the end
                    break

            # Handle any repos that failed *all* retries (or were invalid URLs)
            for repo_url in batch:
                if repo_url not in processed_in_batch:
                    results[repo_url] = None
                    print(f"adding repo to results after max retries, or was invalid url: {repo_url}")

            end_time = time.time()
            batch_time = end_time - start_time
            cpu_time_used += time_since_start
            real_time_used += batch_time
            batch_time_history.append(batch_time)
            if batch_time_history and len(batch_time_history) > 10:
                print(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
            print(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
            print(f"time taken to process batch {i}: {batch_time:.2f} seconds")
            print(f"Total CPU time used: {cpu_time_used:.2f} seconds")
            print(f"Total real time used: {real_time_used:.2f} seconds")

        return results, {
            'count_403_errors': count_403_errors,
            'count_502_errors': count_502_errors
        }

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("""select repo, repo_source from clean.latest_active_distinct_project_repos where is_active = true"""
                )
            )
        repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

    print(f"number of github urls: {len(github_urls)}")

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
        print("found non-github repos. Getting repo watcher count...")
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
        results_df.to_sql('project_repos_watcher_count', cloud_sql_engine, if_exists='append', index=False, schema='raw')

        with cloud_sql_engine.connect() as conn:
            # capture asset metadata
            preview_query = text("select count(*) from raw.project_repos_watcher_count")
            result = conn.execute(preview_query)
            # Fetch all rows into a list of tuples
            row_count = result.fetchone()[0]

            preview_query = text("select * from raw.project_repos_watcher_count limit 10")
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

# define the asset that gets the boolean isFork for a repo
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_repos_is_fork(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_non_github_repo_is_fork(repo_url, repo_source):

        print(f"processing non-githubrepo: {repo_url}")

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
                print(f"Invalid Bitbucket URL format: {repo_url}")
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
                print(f"Error fetching data from Bitbucket API: {e}")
                return None
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}")
                return None
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return None

        elif repo_source == "gitlab":
            try:
                parts = repo_url.rstrip('/').split('/')
                project_path = "/".join(parts[3:])
                project_path_encoded = requests.utils.quote(project_path, safe='')
            except IndexError:
                print(f"Invalid GitLab URL format: {repo_url}")
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
                print(f"Error fetching data from GitLab API: {e}")
                return None
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}")
                return None
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
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
            print(f"processing batch: {i} - {i + batch_size}")
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
                    print(f"Invalid GitHub URL format: {repo_url}")
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
                print(f"attempt: {attempt}")
                
                try:
                    if cpu_time_used >= cpu_time_limit and real_time_used < real_time_window:
                        extra_delay = (cpu_time_used - cpu_time_limit) / 2
                        extra_delay = max(1, extra_delay)
                        print(f"CPU time limit reached. Delaying for {extra_delay:.2f} seconds.")
                        time.sleep(extra_delay)
                        print(f"resetting cpu_time_used and real_time_used to 0")
                        cpu_time_used = 0
                        real_time_used = 0
                        start_time = time.time()
                    elif real_time_used >= real_time_window and cpu_time_used < cpu_time_limit:
                        print(f"real time limit reached without CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used >= real_time_window and cpu_time_used >= cpu_time_limit:
                        print(f"real time limit reached. CPU time limit reached. Resetting counts.")
                        cpu_time_used = 0
                        real_time_used = 0
                    elif real_time_used < real_time_window and cpu_time_used < cpu_time_limit:
                        print('cpu time limit not reached. Continuing...')

                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                    time_since_start = time.time() - start_time
                    print(f"time_since_start: {time_since_start:.2f} seconds")
                    time.sleep(3)  # Consistent delay

                    # use raise for status to catch errors
                    response.raise_for_status()
                    data = response.json()

                    if 'errors' in data:
                        print(f"Status Code: {response.status_code}")
                        # Extract rate limit information from headers
                        print(" \n resource usage tracking:")
                        rate_limit_info = {
                            'remaining': response.headers.get('x-ratelimit-remaining'),
                            'used': response.headers.get('x-ratelimit-used'),
                            'reset': response.headers.get('x-ratelimit-reset'),
                            'retry_after': response.headers.get('retry-after')
                        }
                        print(f"Rate Limit Info: {rate_limit_info}\n")

                        for error in data['errors']:
                            if error['type'] == 'RATE_LIMITED':
                                reset_at = response.headers.get('X-RateLimit-Reset')
                                if reset_at:
                                    delay = int(reset_at) - int(time.time()) + 1
                                    delay = max(1, delay)
                                    delay = min(delay, max_delay)
                                    print(f"Rate limited.  Waiting for {delay} seconds...")
                                    time.sleep(delay)
                                    continue  # Retry the entire batch
                            else:
                                print(f"GraphQL Error: {error}") #Print all the errors.

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
                                print(f"repo_data is empty for repo: {repo_url}\n")
                    break

                except requests.exceptions.RequestException as e:
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print(f"Status Code: {response.status_code}")
                    # Extract rate limit information from headers
                    print(" \n resource usage tracking:")
                    rate_limit_info = {
                        'remaining': response.headers.get('x-ratelimit-remaining'),
                        'used': response.headers.get('x-ratelimit-used'),
                        'reset': response.headers.get('x-ratelimit-reset'),
                        'retry_after': response.headers.get('retry-after')
                    }
                    print(f"Rate Limit Info: {rate_limit_info}\n")

                    print(f"the error is: {e}\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up.")
                        break
                    # --- Rate Limit Handling (REST API style - for 403/429) ---
                    if isinstance(e, requests.exceptions.HTTPError):
                        if e.response.status_code in (502, 504):
                            count_502_errors += 1
                            print(f"This process has generated {count_502_errors} 502/504 errors in total.")
                            delay = 1
                            print(f"502/504 Bad Gateway. Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)
                            continue
                        elif e.response.status_code in (403, 429):
                            count_403_errors += 1
                            print(f"This process has generated {count_403_errors} 403/429 errors in total.")
                            retry_after = response.headers.get('Retry-After')
                            if retry_after:
                                delay = int(retry_after)
                                print(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                                time.sleep(delay)
                                continue
                            else:
                                delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                                print(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                                time.sleep(delay)
                                continue
                    else:
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                        time.sleep(delay)

                except KeyError as e:
                    print(f"KeyError: {e}. Response: {data}")
                    # Don't append here; handle errors at the end
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    # Don't append here; handle errors at the end
                    break

            # Handle any repos that failed *all* retries (or were invalid URLs)
            for repo_url in batch:
                if repo_url not in processed_in_batch:
                    results[repo_url] = None
                    print(f"adding repo to results after max retries, or was invalid url: {repo_url}")

            end_time = time.time()
            batch_time = end_time - start_time
            cpu_time_used += time_since_start
            real_time_used += batch_time
            batch_time_history.append(batch_time)
            if batch_time_history and len(batch_time_history) > 10:
                print(f"average batch time: {sum(batch_time_history) / len(batch_time_history):.2f} seconds")
            print(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
            print(f"time taken to process batch {i}: {batch_time:.2f} seconds")
            print(f"Total CPU time used: {cpu_time_used:.2f} seconds")
            print(f"Total real time used: {real_time_used:.2f} seconds")

        return results, {
            'count_403_errors': count_403_errors,
            'count_502_errors': count_502_errors
        }

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("""select repo, repo_source from clean.latest_active_distinct_project_repos where is_active = true"""
                )
            )
        repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

    print(f"number of github urls: {len(github_urls)}")

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

    # if non_github_urls is not empty, get watcher count
    if not non_github_results_df.empty:
        print("found non-github repos. Getting repo isFork...")
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
        print("Starting cleanup for 'is_fork' column...")
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
                print(f"Found {num_invalid} non-boolean/non-null values in 'is_fork' column. Converting to Null.")
                # Log the actual invalid values found for debugging
                invalid_values_found = results_df.loc[mask_invalid, 'is_fork'].unique()
                print(f"First 25 invalid values found: {invalid_values_found[:25]}")

                # Set invalid values to np.nan (which pandas handles as Null)
                results_df.loc[mask_invalid, 'is_fork'] = np.nan

            # Explicitly convert the column to pandas nullable boolean dtype
            # This helps ensure consistency and correct handling of NA/NaN by to_sql.
            try:
                # Before converting, fill NaN with None if the target SQL type doesn't handle NaN well directly
                # Although SQLAlchemy usually handles np.nan -> NULL correctly for boolean
                results_df['is_fork'] = results_df['is_fork'].where(pd.notna(results_df['is_fork']), None)
                results_df['is_fork'] = results_df['is_fork'].astype('boolean') # Use 'boolean' (Pandas NA) not 'bool'
                print("Successfully converted 'is_fork' column to pandas nullable boolean type.")
            except Exception as e:
                # Log error if conversion fails, but might proceed if np.nan handling is okay
                print(f"Warning: Could not convert 'is_fork' to pandas nullable boolean type: {e}. Proceeding...")

        else:
            print("Warning: 'is_fork' column not found in results_df. Exiting.")
            raise Exception("'is_fork' column not found in results_df. Exiting.")

        # add unix datetime column
        results_df['data_timestamp'] = pd.Timestamp.now()

        # write results to database
        results_df.to_sql('project_repos_is_fork', cloud_sql_engine, if_exists='append', index=False, schema='raw')

        with cloud_sql_engine.connect() as conn:
            # capture asset metadata
            preview_query = text("select count(*) from raw.project_repos_is_fork")
            result = conn.execute(preview_query)
            # Fetch all rows into a list of tuples
            row_count = result.fetchone()[0]

            preview_query = text("select * from raw.project_repos_is_fork limit 10")
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

# define the asset that gets the contributors for a repo
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_repos_contributors(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_next_page(response):
        next_page = response.headers.get('Link')
        if next_page:
            links = next_page.split(',')
            for link in links:
                if 'rel="next"' in link:
                    next_page_url = urlparse(link.split(';')[0].strip('<>')).geturl()
                    next_page_url = next_page_url.replace('<', '').replace('>', '')
                    print(f"Next page found: {next_page_url}")
                    return next_page_url
            print("No next page found")
        else:
            print("Link header not found in response. No next page found.")
        return None

    def get_contributors_list(owner, repo_name, gh_pat):
        """
        Fetches contributors for a repository, handles pagination, and respects rate limits.

        Args:
        owner: Repository owner.
        repo_name: Repository name.
        headers: Request headers.

        Returns:
            The list of contributors.
        """
        api_url = f"https://api.github.com/repos/{owner}/{repo_name}/contributors"
        contributors_list = []
        page_num = 1
        per_page = 100
        max_retries = 5
        count_403_errors = 0
        count_502_errors = 0

        for attempt in range(max_retries):
            pagination_successful_this_attempt = True
            return_204_no_contributors = False

            while api_url is not None:
                print(f"Fetching page {page_num} of contributors from {api_url}")

                try:
                    # Prepare the request headers with your GitHub PAT
                    # Prepare headers (only auth and accept)
                    headers = {
                        "Authorization": f"Bearer {gh_pat}",
                        "Accept": "application/vnd.github.v3+json"
                    }

                    # Prepare parameters for pagination
                    params = {
                        "per_page": per_page,
                        "anon": "true"
                    }

                    # standard time delay
                    time.sleep(1)

                    # Make the request
                    response = requests.get(api_url, headers=headers, params=params)
                    
                    # get next page url
                    if response is not None:
                        if response.status_code == 204:
                            print("No contributors found.")
                            api_url = None
                            return_204_no_contributors = True
                            break
                    
                        # get next page url
                        next_page_url = get_next_page(response)

                    # Now proceed with processing the response
                    response.raise_for_status()

                    contributors = response.json()

                    # Only extend if contributors is not None and is a list
                    if isinstance(contributors, list):
                        contributors_list.extend(contributors)
                        print(f"Fetched {len(contributors_list)} contributors so far")
                    else:
                        print(f"Warning: Expected a list of contributors, got {type(contributors)}. Response: {contributors}")
                        # Decide how to handle this - stop? continue? For now, stop pagination.
                        api_url = None
                        pagination_successful_this_attempt = False # Mark as potentially incomplete
                        continue
                    page_num += 1
                    api_url = next_page_url

                except requests.exceptions.RequestException as e:
                    # Log the error and attempt number
                    print(f"\nError during request for {api_url} on attempt {attempt + 1}/{max_retries}: {e}\n")
                    pagination_successful_this_attempt = False

                    # Safely get status_code and headers for logging and decisions
                    status_code = getattr(getattr(e, 'response', None), 'status_code', None)
                    headers = getattr(getattr(e, 'response', None), 'headers', {})
                    print(f"Status Code (if available): {status_code}")
                    # (You can add back the rate limit info logging here if desired)

                    # ===== CHECK FOR MAX RETRIES =====
                    if attempt == max_retries - 1:
                        print(f"Max retries ({max_retries}) reached for {api_url}. Giving up on this repository.")
                        api_url = None # Set api_url to None to stop pagination loop AFTER this failed attempt
                        # Break from the inner while loop. Since this was the last attempt,
                        # the outer for loop will also terminate naturally.
                        break
                    # ==================================

                    # --- DETERMINE DELAY (if not max retries) ---
                    delay = 1 # Default delay
                    if status_code in (403, 429):
                        count_403_errors += 1
                        retry_after = headers.get('Retry-After')
                        delay = int(retry_after) if retry_after else (1 * (2 ** attempt) + random.uniform(0, 1))
                        print(f"Rate limited (Status {status_code}). Waiting for {delay:.2f} seconds before next attempt...")
                    elif status_code in (502, 504):
                        count_502_errors += 1
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"Server error (Status {status_code}). Waiting for {delay:.2f} seconds before next attempt...")
                    else: # Other RequestException or non-specific HTTPError
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"Request error. Waiting for {delay:.2f} seconds before next attempt...")

                    time.sleep(delay)

                    # --- How to Retry ---
                    # retry loop outside pagination loop
                    # breaking the inner loop means the next attempt will restart pagination from page 1.
                    print("Breaking inner pagination loop to proceed to the next retry attempt.")
                    # We break the 'while' loop here. The 'for attempt' loop will then go to the next iteration.
                    # This means the next attempt restarts pagination for this repo.
                    break

                # Make sure they also set api_url = None and break if they should cause the function to give up on the repo.
                except KeyError as e:
                    print(f"KeyError processing response: {e}") # Avoid printing potentially large 'response' object
                    pagination_successful_this_attempt = False
                    api_url = None # Give up
                    break
                except Exception as e:
                    print(f"An unexpected error occurred processing response: {e}")
                    pagination_successful_this_attempt = False
                    api_url = None # Give up
                    break

            # Check if pagination completed successfully this attempt
            if pagination_successful_this_attempt and api_url is None:
                # The 'while' loop finished because api_url became None naturally (not via error break)
                print(f"Pagination completed successfully for {owner}/{repo_name} on attempt {attempt + 1}.")
                break # <<<--- EXIT THE OUTER 'for attempt:' LOOP ---<<<
            elif attempt == max_retries - 1:
                # This attempt failed (pagination_successful flag is False), and it was the last attempt.
                print(f"Failed to fetch all pages for {owner}/{repo_name} after {max_retries} attempts.")
                # No break needed, outer 'for' loop terminates naturally.
            elif api_url is None and return_204_no_contributors:
                print(f"No contributors found for {owner}/{repo_name} after {attempt + 1} attempts.")
                break
            else:
                # This attempt failed, but more retries remain.
                print(f"Attempt {attempt + 1} failed, proceeding to next attempt.")
                # Let the outer 'for' loop continue.

        # End of 'for attempt...' loop
        return contributors_list, count_403_errors, count_502_errors

    # Fetch all repo names from the database
    with cloud_sql_engine.connect() as conn:

        query = text('''
                            select repo, repo_source 
                            from clean.latest_active_distinct_project_repos 
                            where is_active = true
                        ''')
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        github_repos = df[df['repo_source'] == 'github']['repo'].tolist()

    # extract the repo names from repo_names list
    # only get the repo name from the url: everything after https://github.com/
    repo_names_no_url = [url.split("https://github.com/")[1] for url in github_repos]

    print(f"number of github repos: {len(github_repos)}")

    project_contributors = []
    count_403_errors = 0
    count_502_errors = 0
    count_403_errors_sum = 0
    count_502_errors_sum = 0
    for i in range(len(github_repos)):  # Loop through all repos
        try:
            print(f"\n Processing {i} of {len(github_repos)}")
            print(f"Repo URL: {github_repos[i]}")

            owner = repo_names_no_url[i].split("/")[0]
            repo_name = repo_names_no_url[i].split("/")[1]

            # get the list of contributors
            contributors_list, count_403_errors, count_502_errors = get_contributors_list(owner, repo_name, gh_pat)

            # track http errors
            count_403_errors_sum += count_403_errors
            count_502_errors_sum += count_502_errors

            # Compresses contributor data
            contributors_json = json.dumps(contributors_list).encode('utf-8')
            compressed_contributors_data = gzip.compress(contributors_json)

            # add contributor list to the contributors_list array and associate with repo_url
            project_contributors.append({"repo": github_repos[i], "contributor_list": compressed_contributors_data})

        except Exception as e:
            print(f"Error processing {github_repos[i]}: {e}")

    # Create DataFrame from the list
    project_contributors_df = pd.DataFrame(project_contributors)
    
    # add unix datetime column
    project_contributors_df['data_timestamp'] = pd.Timestamp.now()

    # write results to database
    project_contributors_df.to_sql('project_repos_contributors', cloud_sql_engine, if_exists='append', index=False, schema='raw')

    with cloud_sql_engine.connect() as conn:
        # capture asset metadata
        preview_query = text("select count(*) from raw.project_repos_contributors")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from raw.project_repos_contributors limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
                "count_403_errors": dg.MetadataValue.int(count_403_errors_sum),
                "count_502_errors": dg.MetadataValue.int(count_502_errors_sum),
            }
        )
