import dagster as dg
from dagster_dbt import DbtCliResource, DbtCliInvocation  # Import DbtCliInvocation
import os
import shutil
import subprocess
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from dagster import Output, op, job, Out, Failure, asset
from dagster_pipelines.api_data import create_dbt_api_views

@op(required_resource_keys={"cloud_sql_postgres_resource"})
def create_temp_prod_schema(context, _):
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS temp_prod CASCADE;"))
        conn.execute(text("CREATE SCHEMA temp_prod;"))
        conn.commit()
    context.log.info("Created temp_prod schema.")
    yield Output(None) # Use yield

@op(required_resource_keys={"cloud_sql_postgres_resource"})
def copy_clean_to_temp_prod(context, _):
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        tables = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'clean';")).fetchall()
        tables = [table[0] for table in tables]
        for table_name in tables:
            context.log.info(f"Copying table: clean.{table_name} to temp_prod.{table_name}")
            conn.execute(text(f"""
                CREATE TABLE temp_prod.{table_name} AS
                SELECT * FROM clean.{table_name};
            """))
        conn.commit()
    yield Output(None) # Use yield

@op(required_resource_keys={"dbt_resource"}, out={"dbt_test_results": Out()})
def run_dbt_tests_on_clean(context) -> DbtCliInvocation: #Correct return type
    """Runs dbt tests against the clean schema."""
    context.log.info("Running dbt tests on clean schema for refresh_prod_schema job.")
    try:
        invocation: DbtCliInvocation = context.resources.dbt_resource.cli(
            ["test", "--select", "path:models/clean/"], context=context
        ).wait()
        print(f"dbt test invocation stdout: {type(invocation)}")
        print(f"dbt test invocation stdout: {invocation}")
        print(f"the returncode is {invocation.process.returncode}")
        if invocation.process.returncode == 0:  # Correct way to check for success
            context.log.info("dbt tests on clean schema passed.")
            yield Output(True, output_name="dbt_test_results")
        else:
            context.log.info(f"dbt test invocation stdout: {invocation._stdout}")
            context.log.error(f"dbt test invocation stdout: {invocation._error_messages}") #Correct way to get output
            yield Output(False, output_name="dbt_test_results") # Still return a value

    except Exception as e:
        context.log.error(f"Error running dbt tests: {e}")
        yield Output(False, output_name="dbt_test_results")  # Consistent return type


@op(required_resource_keys={"cloud_sql_postgres_resource"})
def swap_schemas(context, _):
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        conn.execute(text("ALTER SCHEMA prod RENAME TO prod_old;"))
        conn.execute(text("ALTER SCHEMA temp_prod RENAME TO prod;"))
        conn.commit()
    context.log.info("Schemas swapped successfully.")
    yield Output(None) # Use yield

@op(required_resource_keys={"cloud_sql_postgres_resource"})
def cleanup_old_schema(context, _):
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS prod_old CASCADE;"))
        conn.commit()
    context.log.info("Cleaned up prod_old schema.")
    yield Output(None) # Use yield

@job(
    tags={"github_api": "True"},
)
def refresh_prod_schema():
    """
    Refreshes the 'prod' schema from the 'clean' schema with error handling.
    """
    dbt_results = run_dbt_tests_on_clean()
    if dbt_results:
        temp_schema = create_temp_prod_schema(dbt_results)
        copy_data = copy_clean_to_temp_prod(temp_schema)
        schemas_swapped = swap_schemas(copy_data)

        # Build API models AFTER swap, targeting the 'api' schema
        # this is necessary because we use 'cascade' in cleanup_old_schema function
        # which deletes the api schema views since they are dependent on the prod schema tables
        print("Running dbt to create/update views in 'api' schema from refresh_prod_schema job.")
        api_build_invocation = create_dbt_api_views(schemas_swapped) # Op targets API schema

        # now we can safely delete the old prod schema
        cleanup_old_schema(api_build_invocation)


# define the asset that clones/updates the crypto-ecosystems repo and runs the export script
@asset(
    description="Clones/updates the crypto-ecosystems repo and runs the export script.", 
    required_resource_keys={"electric_capital_ecosystems_repo"}
)
def update_crypto_ecosystems_repo_and_run_export(context):
    """
    Clones the repo if it doesn't exist, pulls updates if it does,
    then runs './run.sh export exports.jsonl' inside the repo directory.
    Outputs the full path to the generated exports.jsonl file.
    """
    # define the variables
    try:
        # define the clone repo url
        git_repo_url = context.resources.electric_capital_ecosystems_repo['git_repo_url']
        # define the local path to the cloned repo
        clone_dir = os.path.join(context.resources.electric_capital_ecosystems_repo["clone_parent_dir"], context.resources.electric_capital_ecosystems_repo["repo_name"])
        # define output filename
        output_filename = context.resources.electric_capital_ecosystems_repo['output_filename']
        # define the local path to the output file
        output_filepath = context.resources.electric_capital_ecosystems_repo['output_filepath']
        # clone parent directory
        clone_parent_dir = context.resources.electric_capital_ecosystems_repo['clone_parent_dir']
        # repo name
        repo_name = context.resources.electric_capital_ecosystems_repo['repo_name']
        # primary branch
        primary_branch = context.resources.electric_capital_ecosystems_repo['primary_branch']
    except Exception as e:
        context.log.error(f"Error defining variables: {e}")
        raise Failure(f"Error defining variables: {e}") from e

    # check if the directory exists
    repo_exists = os.path.isdir(clone_dir)

    try:
        if repo_exists:
            context.log.info(f"Repository found at {clone_dir}. Fetching and resetting to origin/{primary_branch}...")
               # Fetch latest changes from remote ('origin' is the default remote name)
            subprocess.run(
                ["git", "fetch", "origin"],
                cwd=clone_dir, check=True, capture_output=True, text=True
            )
            # Reset local branch hard to match the fetched remote primary branch state
            # This discards ALL local changes (committed or uncommitted)
            subprocess.run(
                ["git", "reset", "--hard", f"origin/{primary_branch}"],
                cwd=clone_dir, check=True, capture_output=True, text=True
            )
            context.log.info(f"Repository reset to origin/{primary_branch}.")
        else:
            context.log.info(f"Cloning repository {git_repo_url} to {clone_dir}...")
            # Ensure parent directory exists
            os.makedirs(clone_parent_dir, exist_ok=True)
            subprocess.run(["git", "clone", git_repo_url, clone_dir], check=True, capture_output=True, text=True)
            context.log.info("Repository cloned.")

        # Define paths
        script_path = os.path.join(clone_dir, "run.sh")
        # Assume output file is created in the root of the cloned repo
        output_file_path = os.path.join(clone_dir, output_filename)
        export_command = [script_path, "export", output_filename]

        context.log.info(f"Running export script: {' '.join(export_command)} in {clone_dir}")

        # Execute ./run.sh export exports.jsonl inside the cloned directory
        script_result = subprocess.run(
            export_command,
            cwd=clone_dir,        # <<< Run from the repository's root
            check=True,           # Raise exception on non-zero exit code
            capture_output=True,  # Capture stdout/stderr
            text=True             # Decode output as text
        )
        context.log.info(f"Export script completed successfully.")
        context.log.debug(f"Script stdout:\n{script_result.stdout}")
        if script_result.stderr:
             context.log.warning(f"Script stderr:\n{script_result.stderr}")

        # Verify output file exists
        if not os.path.exists(output_file_path):
            raise Failure(f"Export script ran successfully but output file not found at: {output_file_path}")

        context.log.info(f"Generated export file: {output_file_path}")
        context.log.info(f"Confirming resource output_filepath matches generated file path: {output_filepath}")
        yield Output(output_file_path) # Output the full path

    except subprocess.CalledProcessError as e:
        context.log.error(f"Git or script command failed with exit code {e.returncode}")
        context.log.error(f"Command: '{' '.join(e.cmd)}'")
        context.log.error(f"Stderr: {e.stderr}")
        context.log.error(f"Stdout: {e.stdout}")
        raise Failure(f"Git or script command failed. See logs.") from e
    except Exception as e:
        context.log.error(f"An unexpected error occurred: {e}")
        raise Failure(f"An unexpected error occurred: {e}") from e
