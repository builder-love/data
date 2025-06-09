import os
import re
import pandas as pd
import dagster as dg
from dagster import AssetKey, AssetIn
from sqlalchemy import text
import sqlalchemy

# Define the keyword groups and their corresponding feature column names
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
    "is_eip_erc": ["eip", "erc"],
    "has_examples": ["example", "examples"],
    "is_feature_description": ["feature description"],
    "is_starter_project": ["getting started", "quickstart", "starter project", "starter-kit"],
    "is_guide": ["guide"],
    "is_hackathon_project": ["hackathon"],
    "is_hello_world": ["hello-world", "hello world"],
    "uses_json_rpc": ["json-rpc", "json rpc"],
    "is_interview_related": ["interview"],
    "is_learning_material": ["learn solidity", "learning"],
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

REPO_NAME_FEATURE_MAP = {
    "name_is_example": ["example"],
    "name_is_hello_world": ["hello-world"],
    "name_is_whitepaper": ["whitepaper"],
    "name_is_tutorial": ["tutorial"],
    "name_is_boilerplate": ["boilerplate"],
    "name_is_scaffold": ["scaffold"],
    "name_is_template": ["template"],
    "name_is_kit": ["kit"],
    "name_is_starter": ["starter"],
    "name_is_getting_started": ["getting started"],
    "name_is_quickstart": ["quickstart"],
    "name_is_guide": ["guide"],
    "name_is_hackathon": ["hackathon"],
    "name_is_bootcamp": ["bootcamp"],
    "name_is_course": ["course"],
    "name_is_workshop": ["workshop"],
    "name_is_interview": ["interview"],
}

def create_project_repos_description_features_asset(env_prefix: str):
    """
    Factory function to create an asset that generates boolean features
    from repository descriptions and READMEs based on keyword matching.
    The primary source is the latest_active_distinct_project_repos table.
    """

    @dg.asset(
        key_prefix=env_prefix,
        name="project_repos_description_features",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config"},
        group_name="feature_engineering",
        description="Generates boolean features from repository descriptions and READMEs based on keyword matching.",
        tags={"feature_engineering": "True"}
    )
    def _project_repos_description_features_env_specific(context: dg.OpExecutionContext) -> dg.MaterializeResult:
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        clean_schema = env_config["clean_schema"]

        active_repos_table = "latest_active_distinct_project_repos"
        description_table = "latest_project_repos_description"
        readme_table = "latest_project_repos_readmes"
        output_table_name = "project_repos_features"

        context.log.info(f"Process is running in {env_config['env']} environment.")
        context.log.info(f"Using '{active_repos_table}' as the base and joining descriptions and READMEs.")

        # Read the input tables by joining descriptions and readmes to the active repos list
        try:
            with cloud_sql_engine.connect() as conn:
                query = text(f"""
                    SELECT
                        a.repo,
                        a.data_timestamp,
                        d.description,
                        r.readme_content
                    FROM {clean_schema}.{active_repos_table} AS a
                    LEFT JOIN {clean_schema}.{description_table} AS d ON a.repo = d.repo
                    LEFT JOIN {clean_schema}.{readme_table} AS r ON a.repo = r.repo
                    where a.is_active = true 
                    -- and a.is_archived <> true
                """)
                features_df = pd.read_sql_query(query, conn)
            context.log.info(f"Successfully read {len(features_df)} rows from '{active_repos_table}'.")
        except Exception as e:
            context.log.error(f"Failed to read and join tables: {e}")
            raise

        if features_df.empty:
            context.log.warning(f"No data found in {clean_schema}.{active_repos_table}. No features to generate.")
            return dg.MaterializeResult(
                metadata={
                    "row_count": 0,
                    "preview": dg.MetadataValue.md("Input table was empty. No features generated.")
                }
            )

        # Create the has_readme feature BEFORE filling NaNs
        features_df['has_readme'] = features_df['readme_content'].notna()

        # Fill NaN descriptions and readmes with empty strings to avoid errors
        features_df['description'] = features_df['description'].fillna('')
        features_df['readme'] = features_df['readme_content'].fillna('')

        # generate boolean features from repo name
        for feature_name, keywords in REPO_NAME_FEATURE_MAP.items():
            context.log.debug(f"Generating feature: {feature_name} using keywords: {keywords}")
            pattern = r"\b(" + "|".join(re.escape(kw) for kw in keywords) + r")\b"
            features_df[feature_name] = features_df['repo'].str.contains(pattern, case=False, regex=True, na=False)
        
        context.log.info(f"Generated {len(REPO_NAME_FEATURE_MAP)} boolean feature columns.")

        # Generate boolean features by checking both description and readme
        for feature_name, keywords in KEYWORD_FEATURE_MAP.items():
            context.log.debug(f"Generating feature: {feature_name} using keywords: {keywords}")
            pattern = r"\b(" + "|".join(re.escape(kw) for kw in keywords) + r")\b"
            
            in_description = features_df['description'].str.contains(pattern, case=False, regex=True, na=False)
            in_readme = features_df['readme_content'].str.contains(pattern, case=False, regex=True, na=False)
            
            features_df[feature_name] = in_description | in_readme

        context.log.info(f"Generated {len(KEYWORD_FEATURE_MAP)} boolean feature columns.")

        # Drop the original text columns as they are no longer needed
        features_df = features_df.drop(columns=['description', 'readme_content'])

        # Define dtypes for the output table
        output_dtype_mapping = {
            'repo': sqlalchemy.types.Text,
            'data_timestamp': sqlalchemy.types.TIMESTAMP(timezone=False),
            'has_readme': sqlalchemy.types.Boolean
        }
        for feature_name in KEYWORD_FEATURE_MAP.keys():
            output_dtype_mapping[feature_name] = sqlalchemy.types.Boolean

        # Write the augmented DataFrame to the new table
        try:
            features_df.to_sql(
                output_table_name,
                cloud_sql_engine,
                schema=clean_schema,
                if_exists='replace',
                index=False,
                dtype=output_dtype_mapping
            )
            context.log.info(f"Successfully wrote {len(features_df)} rows with features to {clean_schema}.{output_table_name}")
        except Exception as e:
            context.log.error(f"Failed to write features to {clean_schema}.{output_table_name}: {e}")
            raise

        # Metadata for Dagster UI
        row_count = len(features_df)
        preview_df = features_df.head(10)
        
        preview_columns = ['repo', 'has_readme'] + list(KEYWORD_FEATURE_MAP.keys())[:4]
        actual_preview_columns = [col for col in preview_columns if col in preview_df.columns]

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "num_features_generated": dg.MetadataValue.int(len(KEYWORD_FEATURE_MAP) + 1),
                "output_table": dg.MetadataValue.text(f"{clean_schema}.{output_table_name}"),
                "preview": dg.MetadataValue.md(preview_df[actual_preview_columns].to_markdown(index=False))
            }
        )

    return _project_repos_description_features_env_specific