import os
from pathlib import Path
from sqlalchemy import create_engine
from dagster import resource, EnvVar, ConfigurableResource
from dagster_dbt import DbtCliResource
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import exceptions

# define the cloud sql postgres resource
@resource(
    config_schema={
        "username": str,
        "password": str,
        "hostname": str,
        "database": str,
    }
)
def cloud_sql_postgres_resource(context):
    # Construct the connection string
    conn_str = (
        f"postgresql+psycopg2://"
        f"{context.resource_config['username']}:{context.resource_config['password']}@"
        f"{context.resource_config['hostname']}/{context.resource_config['database']}"
    )

    # Create the engine
    engine = create_engine(conn_str)
    return engine

DBT_PROJECT_DIR = "./dbt-pipelines"
DBT_PROFILES_DIR = DBT_PROJECT_DIR # Assuming profiles.yml is in the dbt project directory

# dbt resource for STAGING environment
# This will use the 'stg' target from your profiles.yml by default if 'target' is not specified,
# or you can explicitly set it.
dbt_stg_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROFILES_DIR),
    target="stg"  # Explicitly set target to 'stg'
)

# dbt resource for PRODUCTION environment
dbt_prod_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROFILES_DIR),
    target="prod"  # Explicitly set target to 'prod'
)

# Resource to get the active environment configuration
@resource(config_schema={"env_target": str}) # Expect "prod" or "stg"
def active_env_config_resource(context):
    env_target = context.resource_config["env_target"]
    config_data = {}  # Initialize an empty dictionary

    if env_target == "stg":
        config_data = {
            "raw_schema": "raw_stg",
            "clean_schema": "clean_stg",
            "temp_target_schema": "temp_prod_stg",
            "target_schema": "prod_stg",
            "target_schema_old": "prod_stg_old",
            "api_schema": "api_stg",
        }
    elif env_target == "prod":
        config_data = {
            "raw_schema": "raw",
            "clean_schema": "clean",
            "temp_target_schema": "temp_prod",
            "target_schema": "prod",
            "target_schema_old": "prod_old",
            "api_schema": "api",
        }
    else:
        raise ValueError(f"Unsupported env_target: {env_target}. Must be 'prod' or 'stg'.")

    # Add the environment name to the returned dictionary using the key 'env'
    config_data['env'] = env_target 

    return config_data

# define the crypto ecosystems repo resource
@resource
def electric_capital_ecosystems_repo():
    # path is relative to the app directory inside the container
    clone_dir = "/opt/dagster/app/data/crypto-ecosystems"

    return {
        "git_repo_url": "https://github.com/electric-capital/crypto-ecosystems.git",
        "clone_parent_dir": clone_dir,
        "repo_name": "crypto-ecosystems",
        "primary_branch": "master",
        "output_filename": "exports.jsonl",
        "output_filepath": os.path.join(clone_dir, "crypto-ecosystems", "exports.jsonl")
    }

# google cloud storage resource
@resource(
    config_schema={
        "gcp_keyfile_path": str
    },
    description="A GCS client that authenticates using a specific service account key file."
)
def gcs_storage_client_resource(context):
    keyfile_path = context.resource_config["gcp_keyfile_path"]
    context.log.info(f"Authenticating GCS client using key file: {keyfile_path}")

    try:
        storage_client = storage.Client.from_service_account_json(keyfile_path)
        # Verify connection by listing buckets (optional but good practice)
        storage_client.list_buckets(max_results=1) 
        context.log.info("GCS client created and authenticated successfully.")
        return storage_client
    except FileNotFoundError:
        context.log.error(f"The specified GCP key file was not found at: {keyfile_path}")
        raise
    except exceptions.DefaultCredentialsError as e:
        context.log.error(f"Credentials error with key file {keyfile_path}: {e}")
        raise

class github_api_resource(ConfigurableResource):
    """A resource for selecting from multiple tokens to access the github API."""

    key_a: str = EnvVar("GITHUB_API_TREBOR")
    key_b: str = EnvVar("GITHUB_API_JACKATJ")

    def get_client(self, key_name: str):
        if key_name == "github_finegrain_trebor":
            api_key = self.key_a
        elif key_name == "github_finegrain_jackatj":
            api_key = self.key_b
        else:
            raise ValueError(f"Unknown key name: {key_name}")

        # Return a configured client or just the key
        print(f"Using API Key: ...{api_key[-4:]}")
        return api_key