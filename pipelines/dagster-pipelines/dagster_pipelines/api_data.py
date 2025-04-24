import dagster as dg
from dagster_dbt import DbtCliResource, DbtCliInvocation
from dagster import Output, op, job, Out, OpExecutionContext
import os
import json

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
def create_dbt_api_views(context, start_after=None) -> DbtCliInvocation:
    """Runs dbt to create/update views in the 'api' schema."""
    context.log.info("Running dbt to create/update views in 'api' schema.")
    try:
        invocation: DbtCliInvocation = context.resources.dbt_resource.cli(
            ["run", "--select", "path:models/api/"], context=context # select the api models
        ).wait()

        # print the models found in the path and their exection status
        executed_model_names = []
        try:
            # Access the structured results from the run
            run_results = invocation.get_artifact("run_results.json")
            context.log.debug(f"dbt run results: {run_results}") 

            for result in run_results.get("results", []):
                # Check if the node is a model and if it completed successfully (or just ran)
                node_type = result.get("node", {}).get("resource_type")
                node_status = result.get("status")
                unique_id = result.get("unique_id", "unknown.id")

                if node_type == "model":
                    model_name = unique_id.split('.')[-1] # Get model name from unique_id
                    executed_model_names.append(model_name)
                    context.log.info(f"Model processed: {model_name} (Status: {node_status})")

        except Exception as e:
            context.log.warning(f"Could not parse run_results.json to log executed models: {e}. Raw logs might contain info.")
            context.log.warning(f"Raw dbt logs:\n{invocation.get_all_logs()}")

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