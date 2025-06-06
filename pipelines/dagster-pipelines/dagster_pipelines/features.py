import os
import re
import pandas as pd
import dagster as dg
from dagster import AssetKey, AssetIn
from sqlalchemy import text
import sqlalchemy

# Define the keyword groups and their corresponding feature column names
# Each key is the feature column name
# Each value is a list of terms/phrases to search for.
# Terms will be searched case-insensitively and as whole words/phrases.
KEYWORD_FEATURE_MAP = {
    "is_collection_of_learnings": ["a collection of learnings"],
    "has_app_application": ["app", "application"],
    "is_awesome_curated": ["awesome", "curated"],
    "has_benchmark": ["benchmark", "benchmarking"],
    "is_block_explorer": ["block explorer"],
    "is_boilerplate_scaffold_template": ["boilerplate", "scaffold", "template"],
    "is_bootcamp": ["bootcamp"],
    "is_bot": ["bot"],
    "has_bounty_program": ["bounty", "bounty program"],
    "has_brand_icon_logo": ["brand", "icon", "logo"],
    "is_cli_tool": ["cli", "cli tool"],
    "is_library": ["client library", "javascript library", "libraries", "library", "python library"],
    "is_course": ["course"],
    "is_demo": ["demo", "this repo demonstrates"],
    "has_docs": ["docs", "documentation", "documentation site", "documents"],
    "is_education_related": ["educate"],
    "is_eip_erc": ["eip", "erc"], # Often uppercase, but regex will be case-insensitive
    "has_examples": ["example", "examples"],
    "is_feature_description": ["feature description"],
    "is_starter_project": ["getting started", "quickstart", "starter project", "starter-kit"],
    "is_guide": ["guide"],
    "is_hackathon_project": ["hackathon"],
    "is_hello_world": ["hello-world", "hello world"], # Added space version
    "uses_json_rpc": ["json-rpc", "json rpc"], # Added space version
    "is_interview_related": ["interview"],
    "is_learning_material": ["learn solidity", "learning"], # 'learnings' is separate
    "is_mcp_server": ["mcp server"],
    "is_plugin": ["plug-in", "plugin"],
    "is_sample_project": ["sample", "sample application", "sample project", "simple example"],
    "is_sdk": ["sdk"],
    "is_security_related": ["security", "exploit", "vulnerability", "honeypot contract", "honeypot"],
    "has_tests_testing": ["test", "testing suite", "tests"],
    "has_tips": ["tips"],
    "is_tooling": ["tool", "toolbox", "toolkit", "tools"],
    "is_tutorial": ["tutorial"],
    "is_whitepaper": ["whitepaper"],
    "is_workshop": ["workshop"],
    "is_wrapper": ["wrapper"]
}

def create_project_repos_description_features_asset(env_prefix: str):
    """
    Factory function to create an asset that generates boolean features
    from repository descriptions based on keyword matching.
    """

    @dg.asset(
        key_prefix=env_prefix,
        name="project_repos_description_features", # Name of the output asset/table
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"},
        group_name="feature_engineering", # Or any group name you prefer
    )
    def _project_repos_description_features_env_specific(context: dg.OpExecutionContext) -> dg.MaterializeResult:
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        # Assuming input is from raw_schema and output goes to clean_schema
        raw_schema = env_config["raw_schema"]
        clean_schema = env_config["clean_schema"] # Output schema

        input_table_name = "project_repos_description" # From your screenshot
        output_table_name = "project_repos_features"

        context.log.info(f"Process is running in {env_config['env']} environment. Generating features for descriptions.")

        # Read the input table (repo and description)
        try:
            with cloud_sql_engine.connect() as conn:
                # to do: join readme and check for patterns
                query = text(f"SELECT repo, description, data_timestamp FROM {raw_schema}.{input_table_name}")
                # If the table is very large, consider fetching in chunks or using server-side processing
                # For 350k rows, pandas should be manageable if memory allows.
                descriptions_df = pd.read_sql_query(query, conn)
            context.log.info(f"Successfully read {len(descriptions_df)} rows from {raw_schema}.{input_table_name}")
        except Exception as e:
            context.log.error(f"Failed to read from {raw_schema}.{input_table_name}: {e}")
            raise

        if descriptions_df.empty:
            context.log.warning(f"No data found in {raw_schema}.{input_table_name}. No features to generate.")
            # Materialize an empty table or handle as appropriate
            return dg.MaterializeResult(
                metadata={
                    "row_count": 0,
                    "num_features_generated": len(KEYWORD_FEATURE_MAP),
                    "preview": dg.MetadataValue.md("Input table was empty. No features generated.")
                }
            )
        
        # Fill NaN descriptions with empty string to avoid errors during regex
        # to do: do the same for readme
        descriptions_df['description'] = descriptions_df['description'].fillna('')

        # Generate boolean features
        for feature_name, keywords in KEYWORD_FEATURE_MAP.items():
            context.log.debug(f"Generating feature: {feature_name} using keywords: {keywords}")
            # Create a regex pattern for the group: \b(keyword1|keyword2|...)\b
            # `re.escape` handles special characters in keywords if any.
            pattern = r"\b(" + "|".join(re.escape(kw) for kw in keywords) + r")\b"
            
            # Apply the regex search (case-insensitive)
            # `.str.contains` returns a boolean Series
            # to do: apply this to readme
            descriptions_df[feature_name] = descriptions_df['description'].str.contains(pattern, case=False, regex=True, na=False)

        context.log.info(f"Generated {len(KEYWORD_FEATURE_MAP)} boolean feature columns.")

        # to do: add boolean column that indicates if the repo has a readme
        # descriptions_df['has_readme'] = descriptions_df['readme'].notna()

        # drop description column
        # to do: do the same for readme
        descriptions_df = descriptions_df.drop(columns=['description'])

        # Define dtypes for the output table, especially for boolean columns
        # to do: add has_readme column dtype
        output_dtype_mapping = {
            'repo': sqlalchemy.types.String(255), # Or TEXT if URLs can be longer
            'data_timestamp': sqlalchemy.types.TIMESTAMP(timezone=True)
        }
        for feature_name in KEYWORD_FEATURE_MAP.keys():
            output_dtype_mapping[feature_name] = sqlalchemy.types.Boolean

        # Write the augmented DataFrame to the new table in clean_schema
        try:
            descriptions_df.to_sql(
                output_table_name,
                cloud_sql_engine,
                schema=clean_schema,
                if_exists='replace', # Or 'append' if you have a different strategy
                index=False,
                dtype=output_dtype_mapping
            )
            context.log.info(f"Successfully wrote {len(descriptions_df)} rows with features to {clean_schema}.{output_table_name}")
        except Exception as e:
            context.log.error(f"Failed to write features to {clean_schema}.{output_table_name}: {e}")
            context.log.error(f"DataFrame info before to_sql: {descriptions_df.info()}")
            context.log.error(f"Sample of DataFrame head: {descriptions_df.head().to_string()}")
            raise

        # Metadata for Dagster UI
        row_count = len(descriptions_df)
        preview_df = descriptions_df.head(10) # Show a preview of the first 10 rows
        
        # Select a subset of columns for a more readable preview if there are many features
        preview_columns = ['repo'] + list(KEYWORD_FEATURE_MAP.keys())[:5] # repo + first 5 features

        # Ensure all selected preview columns exist in preview_df
        actual_preview_columns = [col for col in preview_columns if col in preview_df.columns]


        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "num_features_generated": dg.MetadataValue.int(len(KEYWORD_FEATURE_MAP)),
                "output_table": dg.MetadataValue.text(f"{clean_schema}.{output_table_name}"),
                "preview": dg.MetadataValue.md(preview_df[actual_preview_columns].to_markdown(index=False))
            }
        )

    return _project_repos_description_features_env_specific

