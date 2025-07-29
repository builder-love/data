import dagster as dg
from dagster_dbt import DbtCliResource, DbtCliInvocation
from dagster import Output, op, job, Out, OpExecutionContext
import os
import json

# dbt test op 
@op(
    required_resource_keys={"dbt_cli"}, 
    out={"dbt_test_results": Out()}
    )
def test_dbt_api_views(context: OpExecutionContext, previous_op_output=None) -> DbtCliInvocation:
    """
    Runs dbt tests against the API models using the active dbt target (prod or stg).
    Outputs True if tests pass, False otherwise.
    """
    dbt = context.resources.dbt_cli
    target_name = dbt.target
    context.log.info(f"Running dbt tests on API models (path:models/api/) for dbt target: {target_name}.")

    try:
        # Run tests specifically on the api models
        invocation: DbtCliInvocation = dbt.cli(
            ["test", "--select", "path:models/api/"], 
            context=context
        ).wait()

        if invocation.process.returncode == 0:
            context.log.info(f"dbt tests on API models (target: {target_name}) passed.")
            yield Output(True, output_name="dbt_test_results")
        else:
            context.log.error(f"dbt tests on API models (target: {target_name}) failed: {invocation.raw_logs}")
            yield Output(False, output_name="dbt_test_results")
    except Exception as e:
        context.log.error(f"Error running dbt tests for API models (target: {target_name}): {e}", exc_info=True)
        yield Output(False, output_name="dbt_test_results")

# Op to manage view creations in `api` schema
@op(
    required_resource_keys={"dbt_cli"}, 
    out={"dbt_run_results": Out()}
    )
def create_dbt_api_views(context: OpExecutionContext, start_after=None) -> DbtCliInvocation:
    """
    Runs dbt to create/update views in the target API schema (e.g., 'api' or 'api_stg')
    using the active dbt target. Outputs True if run is successful, False otherwise.
    """
    dbt = context.resources.dbt_cli
    target_name = dbt.target

    context.log.info(f"Running dbt to create/update views in target API schema for dbt target: {target_name} (models: path:models/api/).")
    
    try:
        invocation: DbtCliInvocation = dbt.cli(
            ["run", "--select", "path:models/api/"], context=context # select the api models
        ).wait()

        # print the models found in the path and their exection status
        # Log executed models (optional but good for debugging)
        try:
            run_results = invocation.get_artifact("run_results.json")
            executed_model_names = []
            for result in run_results.get("results", []):
                if result.get("node", {}).get("resource_type") == "model":
                    model_name = result.get("unique_id", "unknown.id").split('.')[-1]
                    executed_model_names.append(model_name)
                    context.log.info(f"Model processed (target: {target_name}): {model_name} (Status: {result.get('status')})")
        except Exception as e:
            context.log.warning(f"Could not parse run_results.json (target: {target_name}): {e}. Raw logs: {invocation.get_all_logs()}")

        if invocation.process.returncode == 0:
            context.log.info(f"dbt run for target API schema (target: {target_name}) successful.")
            yield Output(True, output_name="dbt_run_results")
        else:
            context.log.error(f"dbt run for target API schema (target: {target_name}) failed.")
            yield Output(False, output_name="dbt_run_results")
    except Exception as e:
        context.log.error(f"Error running dbt for target API schema (target: {target_name}): {e}", exc_info=True)
        yield Output(False, output_name="dbt_run_results")

@job(
    tags={"api_data": "True"},
)
def refresh_api_schema():
    """Creates or updates the API views using dbt."""
    run_result = create_dbt_api_views()
    test_results = test_dbt_api_views(run_result) # Pass the results of the run op

    if not run_result or not test_results:
        # Construct a more informative message based on which step failed
        failure_message = []
        if not run_result:
            failure_message.append("dbt run for API schema failed.")
        if not test_results:
            failure_message.append("dbt tests for API schema failed.")
        
        # Use dg.Failure to make it a clear Dagster failure
        raise dg.Failure(description=" ".join(failure_message) + " API views not updated. Check logs.")