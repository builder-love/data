import dagster as dg
from dagster_dbt import DbtCliResource, DbtCliInvocation  # Import DbtCliInvocation
import os
import shutil
import subprocess
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from dagster import Output, op, job, Out, Failure, asset
from .api_data import create_dbt_api_views

@op(required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"})
def create_prod_indexes(context, previous_op_output): 
    """
    Recreates necessary indexes on tables in the temporary target schema
    (e.g., temp_prod or temp_prod_stg). This happens before the swap.
    """

    env_config = context.resources.active_env_config
    temp_target_schema = env_config["temp_target_schema"] # "temp_prod" or "temp_prod_stg"

    context.log.info(f"----------************* Running in environment: {env_config["env"]} *************----------")

    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        # Use CREATE INDEX IF NOT EXISTS to make the op idempotent
        context.log.info(f"Creating b-tree indexes on {temp_target_schema}.latest_top_project_repos...")

        # prod.latest_top_project_repos
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_latest_top_project_repos_project_title ON {temp_target_schema}.latest_top_project_repos (project_title);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_proj_title_repo_rank ON {temp_target_schema}.latest_top_project_repos (project_title, repo_rank);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_proj_title_fork_count ON {temp_target_schema}.latest_top_project_repos (project_title, fork_count);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_proj_title_stargaze_count ON {temp_target_schema}.latest_top_project_repos (project_title, stargaze_count);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_proj_title_watcher_count ON {temp_target_schema}.latest_top_project_repos (project_title, watcher_count);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_proj_title_repo_rank_category ON {temp_target_schema}.latest_top_project_repos (project_title, repo_rank_category);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_proj_title_repo ON {temp_target_schema}.latest_top_project_repos (project_title, repo);
        """))

        context.log.info(f"Creating pg_trgm GIN index on {temp_target_schema}.latest_top_project_repos...")
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_repo_gin_trgm ON {temp_target_schema}.latest_top_project_repos USING gin (repo gin_trgm_ops);
        """))
        context.log.info(f"Created idx_repo_gin_trgm on {temp_target_schema}.latest_top_project_repos.")

        context.log.info(f"Creating pg_trgm GIN index on {temp_target_schema}.normalized_top_projects...")
        # prod.normalized_top_projects
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_proj_title_gin_trgm ON {temp_target_schema}.normalized_top_projects USING gin (project_title gin_trgm_ops);
        """))
        context.log.info(f"Created idx_proj_title_gin_trgm on {temp_target_schema}.normalized_top_projects.")

        # creating hnsw index on latest_project_repo_corpus_embeddings
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_hnsw_latest_project_repo_corpus_embeddings ON {temp_target_schema}.latest_project_repo_corpus_embeddings USING hnsw (corpus_embedding vector_cosine_ops);
        """))
        context.log.info(f"Created idx_hnsw_latest_project_repo_corpus_embeddings on {temp_target_schema}.latest_project_repo_corpus_embeddings.")

        context.log.info(f"Analyzing {temp_target_schema} tables after index creation...")
        conn.execute(text(f"ANALYZE {temp_target_schema}.latest_top_project_repos;"))
        conn.execute(text(f"ANALYZE {temp_target_schema}.normalized_top_projects;"))
        conn.execute(text(f"ANALYZE {temp_target_schema}.latest_project_repo_corpus_embeddings;"))
        context.log.info(f"Successfully analyzed {temp_target_schema}.latest_top_project_repos and {temp_target_schema}.normalized_top_projects.")
        context.log.info(f"Successfully analyzed {temp_target_schema}.latest_project_repo_corpus_embeddings.")

        conn.commit()
    context.log.info(f"Successfully created indexes on {temp_target_schema} tables.")
    return previous_op_output 

@op(required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"})
def create_temp_target_schema(context, previous_op_output):
    """Creates the temporary schema for the target environment (e.g., temp_prod or temp_prod_stg)."""
    env_config = context.resources.active_env_config
    temp_schema_name = env_config["temp_target_schema"]

    context.log.info(f"----------************* Running in environment: {env_config["env"]} *************----------")

    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {temp_schema_name} CASCADE;"))
        conn.execute(text(f"CREATE SCHEMA {temp_schema_name};"))
        conn.commit()
    context.log.info(f"Created {temp_schema_name} schema.")
    return previous_op_output
    # yield Output(None) # Use yield

@op(required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"})
def copy_clean_to_temp_target_schema(context, previous_op_output):
    """Copies tables from the environment's clean schema to its temporary target schema."""
    env_config = context.resources.active_env_config
    clean_schema_name = env_config["clean_schema"]
    temp_target_schema_name = env_config["temp_target_schema"]

    context.log.info(f"----------************* Running in environment: {env_config["env"]} *************----------")

    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        tables = conn.execute(text(f"SELECT tablename FROM pg_tables WHERE schemaname = '{clean_schema_name}';")).fetchall()
        tables = [table[0] for table in tables]
        for table_name in tables:
            context.log.info(f"Copying table: {clean_schema_name}.{table_name} to {temp_target_schema_name}.{table_name}")
            conn.execute(text(f"""
                CREATE TABLE {temp_target_schema_name}.{table_name} AS
                SELECT * FROM {clean_schema_name}.{table_name};
            """))
        conn.commit()
    context.log.info(f"Successfully copied tables from {clean_schema_name} to {temp_target_schema_name}.")
    return previous_op_output
    #yield Output(None) # Use yield

@op(
    required_resource_keys={"dbt_cli"}
)
def run_dbt_tests_on_clean_models(context): #Correct return type
    """
    Runs dbt tests (path:models/clean/) using the active dbt target (prod or stg).
    """
    dbt = context.resources.dbt_cli
    target_name = dbt.target # "prod" or "stg"

    # tell the user what environment we are running in
    context.log.info(f"----------************* Running in environment: {target_name} *************----------")
    
    context.log.info(f"Attempting to run dbt tests on models/clean/ for dbt target: {target_name}.")
    invocation = None
    try:
        # Define the dbt command
        dbt_command = ["test", "--select", "path:models/clean/"]
        context.log.info(f"Executing dbt command: {' '.join(dbt_command)} with target {target_name}")

        # Execute dbt test command and wait for completion
        # The context=context argument ensures logs from the dbt process are captured by Dagster
        invocation: DbtCliInvocation = dbt.cli(
            dbt_command,
            context=context
            # Note: raise_on_error defaults to True. If dbt exits non-zero,
            # DbtCliResource may raise an exception here *before* .wait() returns.
            # This is usually desired behavior.
        ).wait() # Wait for the command to complete

        # *** If execution reaches here, .wait() has returned ***
        context.log.info(f"dbt process (target: {target_name}) completed. Return code: {invocation.process.returncode}. Checking results...")

        # Check the success status using the recommended method
        if invocation.is_successful():
            context.log.info(f"dbt tests on models/clean/ (target: {target_name}) schema passed successfully.")
            # No explicit output needed if just controlling flow
            return # Success, proceed with the job
        else:
            # Tests failed, extract details and raise Failure
            context.log.error(f"dbt test command (target: {target_name}) failed...")
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

@op(required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"})
def swap_target_schemas(context, previous_op_output):
    """Swaps the old target schema with the new temporary target schema."""
    env_config = context.resources.active_env_config
    target_schema = env_config["target_schema"]
    temp_target_schema = env_config["temp_target_schema"]
    target_schema_old = env_config["target_schema_old"]

    context.log.info(f"----------************* Running in environment: {env_config["env"]} *************----------")

    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {target_schema_old} CASCADE;"))
        conn.execute(text(f"ALTER SCHEMA {target_schema} RENAME TO {target_schema_old};"))
        conn.execute(text(f"ALTER SCHEMA {temp_target_schema} RENAME TO {target_schema};"))
        conn.commit()
    context.log.info(f"Schemas swapped successfully: {temp_target_schema} is now {target_schema}. Old schema is {target_schema_old}.")
    return previous_op_output
    # yield Output(None) # Use yield

@op(required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"})
def cleanup_old_target_schema(context, previous_op_output):
    """Cleans up the old target schema after a successful swap."""
    env_config = context.resources.active_env_config
    target_schema_old = env_config["target_schema_old"]

    context.log.info(f"----------************* Running in environment: {env_config["env"]} *************----------")

    cloud_sql_engine = context.resources.cloud_sql_postgres_resource
    with cloud_sql_engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {target_schema_old} CASCADE;"))
        conn.commit()
    context.log.info(f"Cleaned up schema: {target_schema_old}.")
    return previous_op_output
    # yield Output(None) # Use yield

@job(
    tags={"github_api": "True"},
)
def refresh_prod_schema():
    """
    Refreshes the target environment's 'prod' (or 'prod_stg') schema 
    from its 'clean' (or 'clean_stg') schema.
    The specific dbt target and schema names are determined by configured resources.
    """
    dbt_results = run_dbt_tests_on_clean_models()
    temp_schema = create_temp_target_schema(dbt_results)
    copy_data = copy_clean_to_temp_target_schema(temp_schema)
    indexes_created = create_prod_indexes(copy_data)
    schemas_swapped = swap_target_schemas(indexes_created)

    # Build API models AFTER swap, targeting the 'api' schema
    # this is necessary because we use 'cascade' in cleanup_old_target_schema function
    # which deletes the api schema views since they are dependent on the prod schema tables
    api_build_invocation = create_dbt_api_views(schemas_swapped) # Op targets API schema

    # now we can safely delete the old prod schema
    cleanup_old_target_schema(api_build_invocation)


# define the asset that clones/updates the crypto-ecosystems repo and runs the export script
# to accomodate multiple environments, we will use a factory function
def create_update_crypto_ecosystems_repo_and_run_export_asset(env_prefix: str):
    @asset(
        key_prefix=env_prefix,
        name="update_crypto_ecosystems_repo_and_run_export",
        description="Clones/updates the crypto-ecosystems repo and runs the export script.", 
        required_resource_keys={"electric_capital_ecosystems_repo"}
    )
    def _update_crypto_ecosystems_repo_and_run_export(context):
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
            context.log.info(f"Script stdout:\n{script_result.stdout}")
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

    return _update_crypto_ecosystems_repo_and_run_export
