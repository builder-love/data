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
    # yield Output(None) # Use yield

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
    #yield Output(None) # Use yield

@op(
    required_resource_keys={"dbt_resource"}
)
def run_dbt_tests_on_clean(context): #Correct return type
    """
    Runs dbt tests against the clean schema.
    Raises dagster.Failure if any tests fail or if an error occurs.
    """
    context.log.info("Attempting to run dbt tests on clean schema (path:models/clean/).")
    invocation = None # Initialize invocation to None
    try:
        # Define the dbt command
        dbt_command = ["test", "--select", "path:models/clean/"]
        context.log.info(f"Executing dbt command: {' '.join(dbt_command)}")

        # Execute dbt test command and wait for completion
        # The context=context argument ensures logs from the dbt process are captured by Dagster
        invocation: DbtCliInvocation = context.resources.dbt_resource.cli(
            dbt_command,
            context=context
            # Note: raise_on_error defaults to True. If dbt exits non-zero,
            # DbtCliResource may raise an exception here *before* .wait() returns.
            # This is usually desired behavior.
        ).wait() # Wait for the command to complete

        # *** If execution reaches here, .wait() has returned ***
        context.log.info(f"dbt process completed. Return code: {invocation.process.returncode}. Checking results...")

        # Check the success status using the recommended method
        if invocation.is_successful():
            context.log.info("dbt tests on clean schema passed successfully.")
            # No explicit output needed if just controlling flow
            return # Success, proceed with the job
        else:
            # Tests failed, extract details and raise Failure
            context.log.error("dbt tests on clean schema failed!")
            # Try to get stdout/stderr for better debugging
            stdout = invocation.get_stdout() or "No stdout captured."
            stderr = invocation.get_stderr() or "No stderr captured."
            error_message = (
                f"dbt test command failed with return code {invocation.process.returncode}.\n"
                f"--- stdout ---\n{stdout}\n"
                f"--- stderr ---\n{stderr}"
            )
            # Raise Failure to halt job execution and report the error clearly in Dagster UI
            raise Failure(description=error_message)
    # Catching a broad Exception to handle unexpected issues during CLI execution or result processing
    except Exception as e:
        context.log.error(f"An unexpected error occurred during or immediately after dbt test execution: {e}", exc_info=True)

        # Try to construct a helpful error message, including dbt output if available
        error_description = f"Error during/after dbt tests: {e}"
        if invocation:
             # If invocation object exists, dbt process likely finished but something else went wrong
             stdout = invocation.get_stdout() or "No stdout captured."
             stderr = invocation.get_stderr() or "No stderr captured."
             error_description = (
                 f"Exception during/after dbt test execution: {e}\n"
                 f"dbt process return code (if available): {getattr(invocation.process, 'returncode', 'N/A')}\n"
                 f"--- stdout ---\n{stdout}\n"
                 f"--- stderr ---\n{stderr}"
             )
        else:
            # If invocation is None, the error likely happened during the .cli() call itself
             error_description = f"Error occurred before dbt process completion could be confirmed: {e}"

        # Raise Failure to ensure the job stops and the error is surfaced
        raise Failure(description=error_description)

@op(required_resource_keys={"cloud_sql_postgres_resource"})
def swap_schemas(context, _):
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        conn.execute(text("ALTER SCHEMA prod RENAME TO prod_old;"))
        conn.execute(text("ALTER SCHEMA temp_prod RENAME TO prod;"))
        conn.commit()
    context.log.info("Schemas swapped successfully.")
    # yield Output(None) # Use yield

@op(required_resource_keys={"cloud_sql_postgres_resource"})
def cleanup_old_schema(context, _):
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS prod_old CASCADE;"))
        conn.commit()
    context.log.info("Cleaned up prod_old schema.")
    # yield Output(None) # Use yield

@job(
    tags={"github_api": "True"},
)
def refresh_prod_schema():
    """
    Refreshes the 'prod' schema from the 'clean' schema with error handling.
    """
    dbt_results = run_dbt_tests_on_clean()
    temp_schema = create_temp_prod_schema(dbt_results)
    copy_data = copy_clean_to_temp_prod(temp_schema)
    schemas_swapped = swap_schemas(copy_data)

    # Build API models AFTER swap, targeting the 'api' schema
    # this is necessary because we use 'cascade' in cleanup_old_schema function
    # which deletes the api schema views since they are dependent on the prod schema tables
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
