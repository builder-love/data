import dagster as dg
from dagster_dbt import DbtCliResource
import os
import pandas as pd
from sqlalchemy import text
from dagster_pipelines.resources import dbt_resource, cloud_sql_postgres_resource

@dg.op(required_resource_keys={"cloud_sql_postgres_resource"})
def create_temp_prod_schema(context):

    # get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # create the temporary schema
    with cloud_sql_engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS temp_prod CASCADE;"))
        conn.execute(text("CREATE SCHEMA temp_prod;"))

    # commit the changes
    conn.commit()

    context.log.info("Created temp_prod schema.")

@dg.op(required_resource_keys={"cloud_sql_postgres_resource"})
def copy_clean_to_temp_prod(context):

    # get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    with cloud_sql_engine.connect() as conn:
        # Get a list of all tables in the 'clean' schema.
        tables = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'clean';")).fetchall()
        tables = [table[0] for table in tables]  # Extract table names

        for table_name in tables:
            context.log.info(f"Copying table: clean.{table_name} to temp_prod.{table_name}")
            # Use CREATE TABLE AS SELECT for efficient copying.
            conn.execute(text(f"""
                CREATE TABLE temp_prod.{table_name} AS
                SELECT * FROM clean.{table_name};
            """))

        # commit the changes
        conn.commit()

        # Add indexes
        # index_sql_statements = conn.execute(text(f"""SELECT indexdef FROM pg_indexes WHERE tablename = '{table_name}' AND schemaname = 'clean';""")).fetchall()
        # for index in index_sql_statements:
        #     index_sql = str(index[0]).replace("clean.", "temp_prod.")
        #     conn.execute(text(index_sql))

@dg.op(required_resource_keys={"dbt_resource"})
def run_dbt_tests_on_clean(context) -> bool:
    """Runs dbt tests against the clean schema.

    Returns:
        bool: True if all tests pass, False otherwise.
    """
    try:
        test_results = context.resources.dbt_resource.test(select="test_name:check_no_recent_data_repos_languages")
        if not test_results.success:
            # Raise an exception to halt the job
            raise dg.Failure("dbt tests on clean schema failed!")
        context.log.info("dbt tests on clean schema passed.")
    except Exception as e:
        context.log.error(f"Error running dbt tests: {e}")
        raise dg.Failure(f"Error during dbt test execution: {e}") # Ensure the job fails.

@dg.op(required_resource_keys={"cloud_sql_postgres_resource"})
def swap_schemas(context):

    # get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # swap the schemas
    with cloud_sql_engine.connect() as conn:
        conn.execute(text("ALTER SCHEMA prod RENAME TO prod_old;"))
        conn.execute(text("ALTER SCHEMA temp_prod RENAME TO prod;"))

    # commit the changes
    conn.commit()

    context.log.info("Schemas swapped successfully.")

@dg.op(required_resource_keys={"cloud_sql_postgres_resource"})
def cleanup_old_schema(context):

    # get the cloud sql postgres resource
    cloud_sql_engine = context.resources.cloud_sql_postgres_resource

    # drop the old schema
    with cloud_sql_engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS prod_old CASCADE;"))

    # commit the changes
    conn.commit()

    context.log.info("Cleaned up prod_old schema.")

@dg.job(resource_defs={"cloud_sql_postgres_resource": cloud_sql_postgres_resource, "dbt_resource": dbt_resource})
def refresh_prod_schema():
    """
    Refreshes the 'prod' schema from the 'clean' schema with error handling.
    """
    # run dbt tests. if passes, the job will continue
    checks_passed = run_dbt_tests_on_clean()

    print(f"Checks passed. Executing refresh.")

    temp_schema = create_temp_prod_schema()
    copy_data = copy_clean_to_temp_prod()
    swap_schemas()
    cleanup_old_schema()