import dagster as dg
from dagster import asset
import os
import requests
import pandas as pd
import toml
from sqlalchemy import text

# define the asset that gets the list of project toml data files from the crypto ecosystems repo
@dg.asset(
    required_resource_keys={"cloud_sql_postgres_resource"},
    group_name="ingestion",
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
    df['data_timestamp'] = pd.Timestamp.now().timestamp()

    df.to_sql('project_toml_files', cloud_sql_engine, if_exists='append', index=False)

    # capture asset metadata
    with cloud_sql_engine.connect() as conn:
        preview_query = text("select count(*) from project_toml_files")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from project_toml_files limit 10")
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
        query = text("select * from latest_project_toml_files")
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # get data from each toml file listed in the df
        all_orgs = []
        def get_toml_data(row):
            toml_data = get_toml_data(row['toml_file_data_url'])
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

        df.apply(get_toml_data, axis=1)  # axis=1 applies the function to each row

        # create org dataframe
        df_org = pd.DataFrame(all_orgs)

        # add unix datetime column
        df_org['data_timestamp'] = pd.Timestamp.now().timestamp()

        df_org.to_sql('project_organizations', cloud_sql_engine, if_exists='append', index=False)

        # capture asset metadata
        preview_query = text("select count(*) from project_organizations")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from project_organizations limit 10")
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
        query = text("select * from latest_project_toml_files")
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # get data from each toml file listed in the df
        all_subecosystems = []
        def get_toml_data(row):
            toml_data = get_toml_data(row['toml_file_data_url'])
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

        df.apply(get_toml_data, axis=1)  # axis=1 applies the function to each row

        # create org dataframe
        df_subecosystems = pd.DataFrame(all_subecosystems)

        # add unix datetime column
        df_subecosystems['data_timestamp'] = pd.Timestamp.now().timestamp()

        df_subecosystems.to_sql('project_sub_ecosystems', cloud_sql_engine, if_exists='append', index=False)

        # capture asset metadata
        preview_query = text("select count(*) from project_sub_ecosystems")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from project_sub_ecosystems limit 10")
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

    # connect to cloud sql postgres
    with cloud_sql_engine.connect() as conn:
        query = text("select * from latest_project_toml_files")
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # get data from each toml file listed in the df
        all_repos = []
        def get_toml_data(row):
            toml_data = get_toml_data(row['toml_file_data_url'])
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
                                }
                            )
                else:
                    {
                        "project_title": project_title,
                        "repo": 'NA',
                    }
            return row

        df.apply(get_toml_data, axis=1)  # axis=1 applies the function to each row

        # create org dataframe
        df_repos = pd.DataFrame(all_repos)

        # add unix datetime column
        df_repos['data_timestamp'] = pd.Timestamp.now().timestamp()

        df_repos.to_sql('project_repos', cloud_sql_engine, if_exists='append', index=False)

        # capture asset metadata
        preview_query = text("select count(*) from project_repos")
        result = conn.execute(preview_query)
        # Fetch all rows into a list of tuples
        row_count = result.fetchone()[0]

        preview_query = text("select * from project_repos limit 10")
        result = conn.execute(preview_query)
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )

