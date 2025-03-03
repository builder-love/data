import os

import time
import dagster as dg
import pandas as pd
from sqlalchemy import text
import requests
import json
import random
from dagster_pipelines.latest_assets import latest_github_project_repos

# define the asset that gets the distinct repo list from the latest_github_project_repos table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="clean_data",
    deps=[latest_github_project_repos],
    automation_condition=dg.AutomationCondition.eager(),
)
def latest_distinct_github_project_repos(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # create latest project toml files data table with the transformed data
        conn.execute(text("""
            DROP TABLE IF EXISTS latest_distinct_project_repos;
            CREATE TABLE IF NOT EXISTS latest_distinct_project_repos AS
            SELECT DISTINCT repo, repo_source, EXTRACT(EPOCH FROM NOW()) as data_timestamp
            FROM latest_project_repos;
        """))
        conn.commit()

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

# define the asset that gets the active, distinct repo list from the latest_distinct_project_repos table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="clean_data",
    deps=[latest_distinct_github_project_repos],
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

    def check_github_repo_exists(repo_url, gh_pat):
        """
        Checks if GitHub repository exists using the GraphQL API.

        Args:
            repo_urls: A list of GitHub repository URLs.

        Returns:
            A dictionary mapping each repository URL to True (exists and accessible)
            or False (doesn't exist or not accessible).
        """
        api_url = "https://api.github.com/graphql"
        headers = {"Authorization": f"bearer {gh_pat}"}
        results = {}  # Store results: {url: True/False}
        batch_size = 100  # Adjust as needed

        for i in range(0, len(repo_urls), batch_size):
            batch = repo_urls[i:i + batch_size]
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
                    results[repo_url] = False
                    continue

                query += f"$owner{j}: String!, $name{j}: String!,"  # Declare variables
                variables[f"owner{j}"] = owner
                variables[f"name{j}"] = name

            query = query.rstrip(",")  # Remove trailing comma
            query += ") {\n"  # Close the variable declaration

            # 2. Construct the query body (using the declared variables)
            for j, repo_url in enumerate(batch):
                query += f"""  repo{j}: repository(owner: $owner{j}, name: $name{j}) {{
                    id
                    isPrivate
                }}\n"""

            query += "}"

            base_delay = 1
            max_delay = 60
            max_retries = 5

            for attempt in range(max_retries):
                print(f"attempt: {attempt}")
                # wait a default 2 seconds before trying a batch
                time.sleep(3)
                
                try:
                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                    
                    # use raise for status to catch errors
                    response.raise_for_status()
                    data = response.json()

                    print(f"Status Code: {response.status_code}")
                    # Extract rate limit information from headers
                    print(" \n resource usage tracking:")
                    rate_limit_info = {
                        'remaining': response.headers.get('x-ratelimit-remaining'),
                        'used': response.headers.get('x-ratelimit-used'),
                        'reset': response.headers.get('x-ratelimit-reset'),
                        'retry_after': response.headers.get('retry-after')
                    }
                    print("Rate Limit Info:", rate_limit_info)

                    if 'errors' in data:
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
                            repo_data = data['data'].get(f'repo{j}')
                            if repo_data:
                                results[repo_url] = True
                            else:
                                results[repo_url] = False
                    break

                except requests.exceptions.RequestException as e:
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print("the error is:")
                    print(e)
                    print("\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up. Error: {e}")
                        for repo_url in batch:
                            results[repo_url] = False
                        break
                    # --- Rate Limit Handling (REST API style - for 403/429) ---
                    if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code in (403, 429):
                        retry_after = e.response.headers.get('Retry-After')
                        if retry_after:  # Use Retry-After if available
                            delay = int(retry_after)
                            print(f"Rate limited (REST - Retry-After). Waiting for {delay} seconds...")
                            time.sleep(delay)
                            continue # Retry the entire batch
                        else:
                            # Fallback to exponential backoff *only* if Retry-After is missing
                            delay = 1 * (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff
                            print(f"Rate limited (REST - Exponential Backoff). Waiting for {delay:.2f} seconds...")
                            time.sleep(delay)
                            continue # Retry the entire batch
                    else:
                        # Handle other request exceptions (non-rate-limit errors)
                        delay = 1 * (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff
                        print(f"Request failed: {e}. Waiting for {delay:.2f} seconds...")
                        time.sleep(delay)
                except KeyError as e:
                    print(f"KeyError: {e}. Response: {data}")
                    for repo_url in batch:
                        results[repo_url] = False
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    for repo_url in batch:
                        results[repo_url] = False
                    break
        return results

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("""select repo, repo_source 
                    from latest_distinct_project_repos"""
                )
            )
        distinct_repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = distinct_repo_df[distinct_repo_df['repo_source'] == 'github']['repo'].tolist()

    # get github pat
    gh_pat = os.getenv('go_blockchain_ecosystem')

    results = check_github_repo_exists(github_urls, gh_pat)

    # write results to pandas dataframe
    results_df = pd.DataFrame(results.items(), columns=['repo', 'is_active'])

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
    results_df['data_timestamp'] = pd.Timestamp.now().timestamp()

    # write the data to the latest_active_distinct_project_repos table
    results_df.to_sql('latest_active_distinct_project_repos', cloud_sql_engine, if_exists='replace', index=False)

    with cloud_sql_engine.connect() as conn:

        # # capture asset metadata
        preview_query = text("select count(*) from latest_active_distinct_project_repos")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from latest_active_distinct_project_repos limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )