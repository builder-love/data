import dagster as dg
from dagster import asset
import os
import requests
import pandas as pd
import toml
from sqlalchemy import text
import re
import time
import random

# define the asset that gets the list of project toml data files from the crypto ecosystems repo
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def crypto_ecosystems_project_toml_files(context) -> dg.MaterializeResult:

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    # get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the list of folder names in crypto ecosystems repo
    def get_github_folders(url):
        """
        Retrieves a list of folder names from a GitHub repository URL.

        Args:
            url: The URL of the GitHub repository.

        Returns:
            A list of folder names.
        """
        response = requests.get(url, headers={"Authorization": f"Bearer {gh_pat}"})
        response.raise_for_status()  # Raise an exception for bad status codes

        folders = []
        for item in response.json():
            if item['type'] == 'dir':
                folders.append(f"{url}/{item['name']}")
        return folders

    def get_toml_files(url):
        """
        Retrieves a list of .toml files from a GitHub repository URL.

        Args:
            url: The URL of the GitHub repository.

        Returns:
            A list of .toml file names.
        """

        # first get current folder for prepending to toml file name
        folder_name = url.split("/")[-1]
        response = requests.get(url, headers={"Authorization": f"Bearer {gh_pat}"})
        response.raise_for_status()

        toml_files = []
        for item in response.json():
            if item['type'] == 'file' and item['name'].endswith('.toml'):
                toml_files.append(f"{folder_name}/{item['name']}")
        return toml_files

    # crypto ecosystems folder list by alphanumeric
    crypto_ecosystems_project_folders_url = "https://api.github.com/repos/electric-capital/crypto-ecosystems/contents/data/ecosystems"
    # build crypto ecosystems .toml file raw data link and stor in database
    crypto_ecosystems_project_folders_data_url = "https://raw.githubusercontent.com/electric-capital/crypto-ecosystems/master/data/ecosystems"

    # get crypto ecosystems folder list
    folders = get_github_folders(crypto_ecosystems_project_folders_url)

    # get the list of .toml files from each folder
    toml_files_list = []
    for folder in folders:
        toml_files_list.append(get_toml_files(folder))

    # build single list of toml data files
    toml_files_list = [item for sublist in toml_files_list for item in sublist]

    # Concatenate each element with the base toml data URL
    toml_files_list = [f"{crypto_ecosystems_project_folders_data_url}/{item}" for item in toml_files_list]

    # write the list of folder urls to a table
    df = pd.DataFrame(toml_files_list, columns=["toml_file_data_url"])

    # add unix datetime column
    df['data_timestamp'] = pd.Timestamp.now()

    df.to_sql('project_toml_files', cloud_sql_engine, if_exists='append', index=False, schema='raw')

    # capture asset metadata
    with cloud_sql_engine.connect() as conn:
        preview_query = text("select count(*) from raw.project_toml_files")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from raw.project_toml_files limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )


# define the asset that gets all the list of github orgs for a project. Use latest project toml files from the project_toml_files table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_orgs(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_toml_data(url):
        """
        Retrieves and parses data from a .toml file at a given URL.

        Args:
            url: The URL of the .toml file.

        Returns:
            A dictionary containing the extracted text data.
        """
        print(f"getting toml data from {url}\n")
        response = requests.get(url, headers={"Authorization": f"Bearer {gh_pat}"})
        response.raise_for_status()
        return toml.loads(response.text)

    # connect to cloud sql postgres
    with cloud_sql_engine.connect() as conn:
        query = text("select toml_file_data_url from clean.latest_project_toml_files")
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # get data from each toml file listed in the df
        all_orgs = []
        def parse_toml_data(row):
            toml_data = get_toml_data(row)
            project_title = toml_data['title']
            for org in toml_data['github_organizations']:
                if 'missing' not in org:
                    all_orgs.append(
                        {
                        "project_title": project_title,
                        "project_organization_url": org,
                        }
                    )
            return row

        df['toml_file_data_url'].apply(parse_toml_data)

        # create org dataframe
        df_org = pd.DataFrame(all_orgs)

        # add unix datetime column
        df_org['data_timestamp'] = pd.Timestamp.now()

        df_org.to_sql('project_organizations', cloud_sql_engine, if_exists='append', index=False, schema='raw')

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
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )


# define the asset that gets all the list of sub ecosystems for a project. Use latest project toml files from the project_toml_files table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_sub_ecosystems(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_toml_data(url):
        """
        Retrieves and parses data from a .toml file at a given URL.

        Args:
            url: The URL of the .toml file.

        Returns:
            A dictionary containing the extracted text data.
        """
        print(f"getting toml data from {url}\n")
        response = requests.get(url, headers={"Authorization": f"Bearer {gh_pat}"})
        response.raise_for_status()
        return toml.loads(response.text)

    # connect to cloud sql postgres
    with cloud_sql_engine.connect() as conn:
        query = text("select toml_file_data_url from clean.latest_project_toml_files")
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # get data from each toml file listed in the df
        all_subecosystems = []
        def parse_toml_data(row):
            toml_data = get_toml_data(row)
            project_title = toml_data['title']
            # Check if 'sub_ecosystems' key exists
            if 'sub_ecosystems' in toml_data:
                # check if the project listed sub ecosystems
                if toml_data['sub_ecosystems']:
                    for eco in toml_data['sub_ecosystems']:
                        # check if a missing tag exists (indicating a 404 from the url)
                        if 'missing' not in eco:
                            all_subecosystems.append(
                                {
                                "project_title": project_title,
                                "sub_ecosystem": eco,
                                }
                            )
            return row

        df['toml_file_data_url'].apply(parse_toml_data)

        # create org dataframe
        df_subecosystems = pd.DataFrame(all_subecosystems)

        # add unix datetime column
        df_subecosystems['data_timestamp'] = pd.Timestamp.now()

        df_subecosystems.to_sql('project_sub_ecosystems', cloud_sql_engine, if_exists='append', index=False, schema='raw')

        # capture asset metadata
        preview_query = text("select count(*) from raw.project_sub_ecosystems")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from raw.project_sub_ecosystems limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )


# define the asset that gets the list of repositories for a project. Use latest project toml files from the project_toml_files table
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
    tags={"github_api": "True"},  # Add the tag to the asset to let the runqueue coordinator know the asset uses the github api
)
def github_project_repos(context) -> dg.MaterializeResult:
    # Get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # get the github personal access token
    gh_pat = os.environ.get("go_blockchain_ecosystem")

    def get_toml_data(url):
        """
        Retrieves and parses data from a .toml file at a given URL.

        Args:
            url: The URL of the .toml file.

        Returns:
            A dictionary containing the extracted text data.
        """
        print(f"getting toml data from {url}\n")
        response = requests.get(url, headers={"Authorization": f"Bearer {gh_pat}"})
        response.raise_for_status()
        return toml.loads(response.text)

    def get_repo_source(repo_url):
        """
        Determines the source (e.g., GitHub, Bitbucket, GitLab) of a repository
        from its URL.

        Args:
            repo_url: The URL of the repository.

        Returns:
            A string representing the source (e.g., "github", "bitbucket", "gitlab"),
            or "unknown" if the source cannot be determined.
        """
        if not isinstance(repo_url, str):  # Handle potential non-string input
            return "unknown"

        # Lowercase the URL for case-insensitive matching
        repo_url = repo_url.lower()

        # Use regular expressions for more robust matching
        if re.search(r"github\.com", repo_url):
            return "github"
        elif re.search(r"bitbucket\.org", repo_url):
            return "bitbucket"
        elif re.search(r"gitlab\.com", repo_url):
            return "gitlab"
        elif re.search(r"sourceforge\.net", repo_url): #Example of adding another source.
            return "sourceforge"
        else:
            return "unknown"

    # connect to cloud sql postgres
    with cloud_sql_engine.connect() as conn:
        query = text("select toml_file_data_url from clean.latest_project_toml_files")
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # get data from each toml file listed in the df
        all_repos = []
        def parse_toml_data(row):
            toml_data = get_toml_data(row)
            project_title = toml_data['title']
            # Check if 'repo' key exists
            if 'repo' in toml_data:
                # check if the project listed repos
                if toml_data['repo']:
                    for repo in toml_data['repo']:
                        if 'missing' not in repo:
                            all_repos.append(
                                {
                                    "project_title": project_title,
                                    "repo": repo['url'],
                                    "repo_source": get_repo_source(repo['url'])
                                }
                            )
                else:
                    {
                        "project_title": project_title,
                        "repo": 'NA',
                    }
            return row

        df['toml_file_data_url'].apply(parse_toml_data)

        # create org dataframe
        df_repos = pd.DataFrame(all_repos)

        # add unix datetime column
        df_repos['data_timestamp'] = pd.Timestamp.now()

        df_repos.to_sql('project_repos', cloud_sql_engine, if_exists='append', index=False, schema='raw')

        # capture asset metadata
        preview_query = text("select count(*) from raw.project_repos")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from raw.project_repos limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
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
                return "NA"

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
                return "NA"
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}")
                return "NA"
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return "NA"

        elif repo_source == "gitlab":
            try:
                parts = repo_url.rstrip('/').split('/')
                project_path = "/".join(parts[3:])
                project_path_encoded = requests.utils.quote(project_path, safe='')
            except IndexError:
                print(f"Invalid GitLab URL format: {repo_url}")
                return "NA"

            api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

            try:
                response = requests.get(api_url)  # No headers needed for unauthenticated access
                response.raise_for_status()

                # return the stargaze count
                return response.json()['star_count']
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from GitLab API: {e}")
                return "NA"
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}") 
                return "NA"
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return "NA"
        else:
            return "NA"

    def get_github_repo_stargaze_count(repo_urls, gh_pat):
        """
        Queries the stargaze count for a GitHub repository using the GraphQL API.

        Args:
            repo_urls: A list of GitHub repository URLs.

        Returns:
            A dictionary mapping each repository URL to the stargaze count.
        """
        api_url = "https://api.github.com/graphql"
        headers = {"Authorization": f"bearer {gh_pat}"}
        results = {}  # Store results: {url: stargaze_count}
        batch_size = 200  # Adjust as needed

        for i in range(0, len(repo_urls), batch_size):
            print(f"processing batch: {i} - {i + batch_size}")
            # calculate the time it takes to process the batch
            start_time = time.time()
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
                    stargazers {{
                        totalCount
                    }}
                }}\n"""

            query += "}"

            base_delay = 1
            max_delay = 60
            max_retries = 5

            for attempt in range(max_retries):
                print(f"attempt: {attempt}")
                # wait a default 3 seconds before trying a batch
                time.sleep(3)
                
                try:
                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                    
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
                        print("Rate Limit Info:", rate_limit_info)

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
                            repo_data = data['data'].get(f'repo{j}')
                            if repo_data:
                                results[repo_url] = repo_data['stargazers']['totalCount']
                            else:
                                results[repo_url] = "NA"
                    break

                except requests.exceptions.RequestException as e:
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print(f"Status Code: {response.status_code}")

                    # Extract rate limit information from headers
                    print(" \n resource usage tracking:")
                    rate_limit_info = {
                        'remaining': e.response.headers.get('x-ratelimit-remaining'),
                        'used': e.response.headers.get('x-ratelimit-used'),
                        'reset': e.response.headers.get('x-ratelimit-reset'),
                        'retry_after': e.response.headers.get('retry-after')
                    }
                    print("Rate Limit Info:", rate_limit_info)

                    print("the error is:")
                    print(e)
                    print("\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up. Error: {e}")
                        for repo_url in batch:
                            results[repo_url] = "NA"
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
                        results[repo_url] = "NA"
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    for repo_url in batch:
                        results[repo_url] = "NA"
                    break

            # calculate the time it takes to process the batch
            end_time = time.time()
            print(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
            print(f"time taken to process batch {i}: {end_time - start_time}")
        return results

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("""select repo, repo_source from raw.latest_active_distinct_project_repos where is_active = true"""
                )
            )
        repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

    # get github pat
    gh_pat = os.getenv('go_blockchain_ecosystem')

    github_results = get_github_repo_stargaze_count(github_urls, gh_pat)

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
                return "NA"

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
                return "NA"
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}")
                return "NA"
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return "NA"

        elif repo_source == "gitlab":
            try:
                parts = repo_url.rstrip('/').split('/')
                project_path = "/".join(parts[3:])
                project_path_encoded = requests.utils.quote(project_path, safe='')
            except IndexError:
                print(f"Invalid GitLab URL format: {repo_url}")
                return "NA"

            api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}"  

            try:
                response = requests.get(api_url)  # No headers needed for unauthenticated access
                response.raise_for_status()

                # return the fork count
                return response.json()['forks_count']
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from GitLab API: {e}")
                return "NA"
            except KeyError as e:
                print(f"Error: missing key in response.  Key: {e}") 
                return "NA"
            except Exception as e:
                print(f"An unexpected error has occurred: {e}")
                return "NA"
        else:
            return "NA"

    def get_github_repo_fork_count(repo_urls, gh_pat):
        """
        Queries the fork count for a GitHub repository using the GraphQL API.

        Args:
            repo_urls: A list of GitHub repository URLs.

        Returns:
            A dictionary mapping each repository URL to the fork count.
        """
        api_url = "https://api.github.com/graphql"
        headers = {"Authorization": f"bearer {gh_pat}"}
        results = {}  # Store results: {url: fork_count}
        batch_size = 200  # Adjust as needed

        for i in range(0, len(repo_urls), batch_size):
            print(f"processing batch: {i} - {i + batch_size}")
            # calculate the time it takes to process the batch
            start_time = time.time()
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
                    forkCount
                }}\n"""

            query += "}"

            base_delay = 1
            max_delay = 60
            max_retries = 5

            for attempt in range(max_retries):
                print(f"attempt: {attempt}")
                # wait a default 3 seconds before trying a batch
                time.sleep(3)
                
                try:
                    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
                    
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
                        print("Rate Limit Info:", rate_limit_info)

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
                            repo_data = data['data'].get(f'repo{j}')
                            if repo_data:
                                results[repo_url] = repo_data['forkCount']
                            else:
                                results[repo_url] = "NA"
                    break

                except requests.exceptions.RequestException as e:
                    print(f"there was a request exception on attempt: {attempt}\n")
                    print(f"procesing batch: {batch}\n")
                    print(f"Status Code: {response.status_code}")
                    # Extract rate limit information from headers
                    print(" \n resource usage tracking:")
                    rate_limit_info = {
                        'remaining': e.response.headers.get('x-ratelimit-remaining'),
                        'used': e.response.headers.get('x-ratelimit-used'),
                        'reset': e.response.headers.get('x-ratelimit-reset'),
                        'retry_after': e.response.headers.get('retry-after')
                    }
                    print("Rate Limit Info:", rate_limit_info)

                    print("the error is:")
                    print(e)
                    print("\n")
                    if attempt == max_retries - 1:
                        print(f"Max retries reached or unrecoverable error for batch. Giving up. Error: {e}")
                        for repo_url in batch:
                            results[repo_url] = "NA"
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
                        results[repo_url] = "NA"
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    for repo_url in batch:
                        results[repo_url] = "NA"
                    break

            # calculate the time it takes to process the batch
            end_time = time.time()
            print(f"batch {i} - {i + batch_size} completed. Total repos to process: {len(repo_urls)}")
            print(f"time taken to process batch {i}: {end_time - start_time}")
        return results

    # Execute the query
    with cloud_sql_engine.connect() as conn:

        # query the latest_distinct_project_repos table to get the distinct repo list
        result = conn.execute(
            text("""select repo, repo_source from raw.latest_active_distinct_project_repos where is_active = true"""
                )
            )
        repo_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Filter for GitHub URLs
    github_urls = repo_df[repo_df['repo_source'] == 'github']['repo'].tolist()

    print(f"number of github urls: {len(github_urls)}")

    # get github pat
    gh_pat = os.getenv('go_blockchain_ecosystem')

    github_results = get_github_repo_fork_count(github_urls, gh_pat)

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
        }
    )