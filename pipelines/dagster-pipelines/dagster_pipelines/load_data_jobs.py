import dagster as dg
from dagster_dbt import DbtCliResource, DbtCliInvocation  # Import DbtCliInvocation
import os
import pandas as pd
from sqlalchemy import text
from dagster import Output, op, job, Out

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
        cleanup_old_schema(schemas_swapped)