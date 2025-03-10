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

# define dbt resource
dbt_resource = DbtCliResource(
    project_dir="/home/builder-love/data/pipelines/dbt-pipelines/dbt_pipelines",  # Absolute path
    profiles_dir="/root/.dbt",
    executable="/home/builder-love/data/pipelines/dbt_venv/bin/dbt"  # Absolute path to dbt in venv
)