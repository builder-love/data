import os
from pathlib import Path
from sqlalchemy import create_engine
from dagster import resource
from dagster_dbt import DbtCliResource

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

# define the path to dbt project directory
# Get the absolute path to the directory containing this file (resources.py)
_THIS_FILE_DIR = Path(__file__).parent.resolve()
_PROJECT_ROOT_PATH = _THIS_FILE_DIR.parent.parent

# this calculates the path relative to the current file (resources.py)
# assumes resources.py -> dagster_pipelines -> dagster-pipelines -> data -> dbt-pipelines/dbt_pipelines
DBT_PROJECT_PATH = _PROJECT_ROOT_PATH / "dbt-pipelines" / "dbt_pipelines"

# Calculate path to the dbt executable inside dbt_venv
DBT_EXECUTABLE_PATH = _PROJECT_ROOT_PATH / "dbt_venv" / "bin" / "dbt"

# define dbt resource
dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_PATH),  
    profiles_dir=os.fspath(DBT_PROJECT_PATH),
    executable=os.fspath(DBT_EXECUTABLE_PATH)
)

# define the crypto ecosystems repo resource
@resource
def electric_capital_ecosystems_repo():
    return {
        "git_repo_url": "https://github.com/electric-capital/crypto-ecosystems.git",
        "clone_parent_dir": os.path.join(os.environ.get("DAGSTER_HOME"), "crypto-ecosystems"),
        "repo_name": "crypto-ecosystems",
        "primary_branch": "master",
        "output_filename": "exports.jsonl",
        "output_filepath": os.path.join(os.environ.get("DAGSTER_HOME"), "crypto-ecosystems", "crypto-ecosystems","exports.jsonl")
    }