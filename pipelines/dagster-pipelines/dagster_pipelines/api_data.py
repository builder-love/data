import dagster as dg
from dagster_dbt import DbtCliResource, DbtCliInvocation
from dagster import Output, op, job, Out
import os

# dbt test op 
@op(required_resource_keys={"dbt_resource"}, out={"dbt_test_results": Out()})
def test_dbt_api_views(context, _) -> DbtCliInvocation:
    """Runs dbt tests against the API schema."""
    context.log.info("Running dbt tests on API schema.")
    try:
        # Run tests specifically on the api models
        invocation: DbtCliInvocation = context.resources.dbt_resource.cli(
            ["test", "--select", "path:models/api/"], context=context
        ).wait()
        if invocation.process.returncode == 0:
            context.log.info("dbt tests on API schema passed.")
            yield Output(True, output_name="dbt_test_results")
        else:
            context.log.error(f"dbt tests on API schema failed: {invocation.raw_logs}")
            yield Output(False, output_name="dbt_test_results")
    except Exception as e:
        context.log.error(f"Error running dbt tests: {e}")
        yield Output(False, output_name="dbt_test_results")

# Op to manage view creations in `api` schema
@op(required_resource_keys={"dbt_resource"}, out={"dbt_run_results": Out()})
def create_dbt_api_views(context) -> DbtCliInvocation:
    """Runs dbt to create/update views in the 'api' schema."""
    context.log.info("Running dbt to create/update views in 'api' schema.")
    try:
        invocation: DbtCliInvocation = context.resources.dbt_resource.cli(
            ["run", "--select", "path:models/api/"], context=context # select the api models
        ).wait()
        if invocation.process.returncode == 0:
            context.log.info("dbt run for 'api' schema successful.")
            yield Output(True, output_name="dbt_run_results")
        else:
            context.log.error(f"dbt run for 'api' schema failed: {invocation.raw_logs}")
            yield Output(False, output_name="dbt_run_results")
    except Exception as e:
        context.log.error(f"Error running dbt for 'api' schema: {e}")
        yield Output(False, output_name="dbt_run_results")

@job(
    tags={"api_data": "True"},
)
def refresh_api_schema():
    """Creates or updates the API views using dbt."""
    run_result = create_dbt_api_views()
    test_results = test_dbt_api_views(run_result) # Pass the results of the run op

    if not run_result or not test_results:
        raise Exception("dbt run or test for API schema failed. API views not updated. Check stdout/stderr for more details.")