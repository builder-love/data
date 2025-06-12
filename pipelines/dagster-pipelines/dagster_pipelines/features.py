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
    "has_app_application": ["app", "application", "dapp", "decentralized app", "decentralized application"],
    "is_awesome_curated": ["awesome", "curated"],
    "has_benchmark": ["benchmark", "benchmarking"],
    "is_block_explorer": ["block explorer"],
    "is_boilerplate_scaffold_template": ["boilerplate", "scaffold", "template", "One-line setup", "One line setup"],
    "is_bootcamp": ["bootcamp"],
    "is_bot": ["bot"],
    "has_bounty_program": ["bounty", "bounty program"],
    "has_brand_icon_logo": ["brand", "icon", "logo"],
    "is_cli_tool": ["cli", "cli tool"],
    "is_library": ["client library", "javascript library", "libraries", "library", "python library"],
    "is_course": ["course"],
    "is_demo": ["demo", "this repo demonstrates", "This project demonstrates"],
    "has_docs": ["docs", "documentation", "documentation site", "documents"],
    "is_education_related": ["educate", "education", "education purposes"],
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
    "is_sample_project": ["sample", "sample application", "sample project", "simple example", "Sample Hardhat Project"],
    "is_sdk": ["sdk"],
    "is_security_related": ["security", "exploit", "vulnerability", "honeypot contract", "honeypot"],
    "has_tests_testing": ["test", "testing suite", "tests", "test environment", "test environment setup"],
    "has_tips": ["tips"],
    "is_tooling": ["tool", "toolbox", "toolkit", "tools"],
    "is_tutorial": ["tutorial"],
    "is_whitepaper": ["whitepaper"],
    "is_workshop": ["workshop"],
    "is_wrapper": ["wrapper"],
    "is_experiment": ["experiment", "experiments", "this is an experiment"],
    "is_research": ["research", "research paper", "research project", "research related", "researching", "researching project", "research project related", "Bachelor thesis", "Master thesis", "PhD thesis", "thesis project", "thesis related", "thesis researching", "thesis project related"]
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

# A comprehensive map for various package manager files.
# This relies on string/regex matching within the file content.
PACKAGE_MANAGER_FEATURE_MAP_BY_FILE = {
    "package.json": { # NPM/Yarn (JSON)
        "pm_has_main_entrypoint": ['"main":'],
        "pm_has_bin_script": ['"bin":'],
        "pm_has_dependencies": ['"dependencies":'],
        "pm_has_version_control": ['"version":'],
        "pm_has_author_cited": ["author", "authors", "contributor", "contributors"],
        "pm_has_license": ['"license":', '"licenses":'],
        "pm_has_repository": ['"repository":', '"repositorie":', '"repo":', '"repos":']
    },
    "Cargo.toml": { # Rust (TOML)
        "pm_has_main_entrypoint": ['[lib]'],
        "pm_has_bin_script": ['[[bin]]'],
        "pm_has_dependencies": ['[dependencies]'],
        "pm_has_version_control": ['version ='], # Typically under [package]
        "pm_has_author_cited": ['authors ='],
        "pm_has_license": ['license ='],
        "pm_has_repository": ['repository =']
    },
    "pom.xml": { # Java Maven (XML)
        "pm_has_main_entrypoint": ['<mainClass>'],
        "pm_has_bin_script": ['maven-assembly-plugin', 'maven-shade-plugin'], # Heuristic for executable jars
        "pm_has_dependencies": ['<dependencies>'],
        "pm_has_version_control": ['<version>'], # This tag is used for both project and dependency versions
        "pm_has_author_cited": ['<developer>', '<contributor>'],
        "pm_has_license": ['<licenses>'],
        "pm_has_repository": ['<scm>'] # Source Control Management
    },
    "pyproject.toml": { # Python (TOML)
        "pm_has_main_entrypoint": ['[project.scripts]', '[project.gui-scripts]'],
        "pm_has_bin_script": ['[project.scripts]', '[project.gui-scripts]'],
        "pm_has_dependencies": ['[project.dependencies]', '[tool.poetry.dependencies]'],
        "pm_has_version_control": ['version ='], # Under [project] or [tool.poetry]
        "pm_has_author_cited": ['authors =', '[project.authors]'],
        "pm_has_license": ['license ='],
        "pm_has_repository": ['[project.urls]', 'repository =', 'homepage =']
    },
    "composer.json": { # PHP (JSON)
        "pm_has_main_entrypoint": ['"autoload"'],
        "pm_has_bin_script": ['"bin"'],
        "pm_has_dependencies": ['"require"'],
        "pm_has_version_control": ['"version":'],
        "pm_has_author_cited": ['"authors":'],
        "pm_has_license": ['"license":'],
        "pm_has_repository": ['"support":', '"homepage":']
    },
    "Gemfile": { # Ruby
        "pm_has_main_entrypoint": [], # Not specified in Gemfile
        "pm_has_bin_script": [], # Not specified in Gemfile
        "pm_has_dependencies": ['gem '],
        "pm_has_version_control": [], # Not specified in Gemfile
        "pm_has_author_cited": [], # Not specified in Gemfile
        "pm_has_license": [], # Not specified in Gemfile
        "pm_has_repository": ['git_source', 'github']
    },
    # This captures metadata for RubyGems
    ".gemspec": {
        "pm_has_main_entrypoint": ['spec.require_paths'],
        "pm_has_bin_script": ['spec.executables'],
        "pm_has_dependencies": ['spec.add_dependency'],
        "pm_has_version_control": ['spec.version'],
        "pm_has_author_cited": ['spec.authors'],
        "pm_has_license": ['spec.license'],
        "pm_has_repository": ['spec.homepage', "spec.metadata['source_code_uri']"]
    },
    "build.gradle": { # Groovy Gradle
        "pm_has_main_entrypoint": ["mainClassName"],
        "pm_has_bin_script": ["application"], # The application plugin creates start scripts
        "pm_has_dependencies": ["dependencies {", "implementation ", "api "],
        "pm_has_version_control": ["version ="],
        "pm_has_author_cited": [], # Not a standard field
        "pm_has_license": ["licenses {"], # From publishing plugin
        "pm_has_repository": ["scm {"] # From publishing plugin
    },
    "build.gradle.kts": { # Kotlin Gradle
        "pm_has_main_entrypoint": ["mainClass.set("],
        "pm_has_bin_script": ["application"],
        "pm_has_dependencies": ["dependencies {", "implementation("],
        "pm_has_version_control": ["version ="],
        "pm_has_author_cited": [],
        "pm_has_license": ["licenses {"],
        "pm_has_repository": ["scm {"]
    },
    "go.mod": { # Go
        "pm_has_main_entrypoint": [], # Not specified in go.mod
        "pm_has_bin_script": [], # Not specified in go.mod
        "pm_has_dependencies": ["require (", "require "],
        "pm_has_version_control": [], # Not specified in go.mod
        "pm_has_author_cited": [], # Not specified in go.mod
        "pm_has_license": [], # Not specified in go.mod
        "pm_has_repository": ["module "] # The module path is the repository
    },
    "setup.py": { # Python Legacy
        "pm_has_main_entrypoint": ["entry_points"],
        "pm_has_bin_script": ["console_scripts"],
        "pm_has_dependencies": ["install_requires"],
        "pm_has_version_control": ["version="],
        "pm_has_author_cited": ["author="],
        "pm_has_license": ["license="],
        "pm_has_repository": ["url="]
    },
     "setup.cfg": { # Python Legacy
        "pm_has_main_entrypoint": ["entry_points"],
        "pm_has_bin_script": ["console_scripts"],
        "pm_has_dependencies": ["install_requires"],
        "pm_has_version_control": ["version ="],
        "pm_has_author_cited": ["author ="],
        "pm_has_license": ["license ="],
        "pm_has_repository": ["url ="]
    },
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

        active_repos_table = "latest_active_distinct_project_repos_with_code"
        description_table = "latest_project_repos_description"
        readme_table = "latest_project_repos_readmes"
        package_files_table = "latest_project_repos_package_files"
        is_fork_table = "latest_project_repos_is_fork"
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
                        r.readme_content,
                        p.file_name,
                        p.file_content,
                        f.is_fork
                    FROM {clean_schema}.{active_repos_table} AS a
                    LEFT JOIN {clean_schema}.{description_table} AS d ON a.repo = d.repo
                    LEFT JOIN {clean_schema}.{readme_table} AS r ON a.repo = r.repo
                    LEFT JOIN {clean_schema}.{package_files_table} AS p ON a.repo = p.repo
                    LEFT JOIN {clean_schema}.{is_fork_table} AS f ON a.repo = f.repo
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

        # prepare data and initialize all feature columns
        features_df['has_readme'] = features_df['readme_content'].notna()
        features_df['has_description'] = features_df['description'].notna()
        features_df['has_package_file'] = features_df['file_content'].notna()
        features_df['is_fork'].fillna(False, inplace=True)

        features_df.fillna({
            'description': '',
            'readme_content': '',
            'file_name': '',
            'file_content': ''
        }, inplace=True)
        
        # Initialize ALL boolean feature columns to False beforehand
        all_feature_names = set(KEYWORD_FEATURE_MAP.keys()) | set(REPO_NAME_FEATURE_MAP.keys())
        pm_feature_names = set()
        for file_map in PACKAGE_MANAGER_FEATURE_MAP_BY_FILE.values():
            pm_feature_names.update(file_map.keys())
        all_feature_names.update(pm_feature_names)

        for feature in all_feature_names:
            features_df[feature] = False
        
        context.log.info(f"Initialized {len(all_feature_names)} total feature columns.")

        # generate all features on the un-aggregated dataframe
        
        # Generate features from Repo Name
        context.log.info("Generating features from repository name...")
        for feature_name, keywords in REPO_NAME_FEATURE_MAP.items():
            pattern = r"\b(" + "|".join(re.escape(kw) for kw in keywords) + r")\b"
            features_df[feature_name] = features_df['repo'].str.contains(pattern, case=False, regex=True)
        context.log.info(f"Generated {len(REPO_NAME_FEATURE_MAP)} boolean feature columns from repo name.")

        # Generate features from Description and README
        context.log.info("Generating features from description and README...")
        for feature_name, keywords in KEYWORD_FEATURE_MAP.items():
            pattern = r"\b(" + "|".join(re.escape(kw) for kw in keywords) + r")\b"
            in_description = features_df['description'].str.contains(pattern, case=False, regex=True)
            in_readme = features_df['readme_content'].str.contains(pattern, case=False, regex=True)
            features_df[feature_name] = in_description | in_readme
        context.log.info(f"Generated {len(KEYWORD_FEATURE_MAP)} boolean feature columns from description and readme.")

        # Generate features from Package Manager Files
        context.log.info("Generating features from package manager files...")
        for file_pattern, mappings in PACKAGE_MANAGER_FEATURE_MAP_BY_FILE.items():
            mask = features_df['file_name'].str.endswith(file_pattern, na=False)
            if not mask.any():
                continue
            
            context.log.info(f"Processing {mask.sum()} files matching '{file_pattern}'...")
            for feature_name, keywords in mappings.items():
                if not keywords:
                    continue
                pattern = "|".join(re.escape(kw) for kw in keywords)
                matches = features_df.loc[mask, 'file_content'].str.contains(pattern, case=False, regex=True)
                features_df.loc[mask & matches, feature_name] = True
        context.log.info(f"Generated {len(pm_feature_names)} unique boolean feature columns from package manager files.")

        # aggregate results to one row per repo
        context.log.info("Aggregating feature data to one row per repository.")
        
        # Define aggregation functions. 'any' acts as a logical OR for boolean columns.
        agg_funcs = { 'data_timestamp': 'first', 'is_fork': 'first' }
        feature_and_flag_cols = list(all_feature_names) + ['has_readme', 'has_description', 'has_package_file']
        for col in feature_and_flag_cols:
             agg_funcs[col] = 'any'
        
        # Select only the columns needed for aggregation
        cols_to_agg = ['repo', 'data_timestamp', 'is_fork'] + feature_and_flag_cols
        final_features_df = features_df[cols_to_agg].groupby('repo').agg(agg_funcs).reset_index()

        context.log.info(f"Aggregation complete. Result has {len(final_features_df)} unique repos.")

        # Define dtypes for the output table
        output_dtype_mapping = {
            'repo': sqlalchemy.types.Text,
            'data_timestamp': sqlalchemy.types.TIMESTAMP(timezone=False),
        }
        for col in final_features_df.columns:
            if col not in output_dtype_mapping and final_features_df[col].dtype == 'bool':
                output_dtype_mapping[col] = sqlalchemy.types.Boolean

        # Write the augmented DataFrame to the new table
        try:
            # perform database operations in a transaction
            with cloud_sql_engine.begin() as conn:
                # replace existing table
                final_features_df.to_sql(
                    output_table_name,
                    conn,
                    schema=clean_schema,
                    if_exists='replace',
                    index=False,
                    dtype=output_dtype_mapping
                )
                context.log.info(f"Successfully wrote {len(final_features_df)} rows with features to {clean_schema}.{output_table_name}")

                # add placeholder columns for prediction fields
                conn.execute(text(f"ALTER TABLE {clean_schema}.{output_table_name} ADD COLUMN predicted_is_scaffold boolean"))
                conn.execute(text(f"ALTER TABLE {clean_schema}.{output_table_name} ADD COLUMN predicted_is_educational boolean"))
                conn.execute(text(f"ALTER TABLE {clean_schema}.{output_table_name} ADD COLUMN predicted_is_dev_tooling boolean"))

        except Exception as e:
            context.log.error(f"Failed to write features to {clean_schema}.{output_table_name}: {e}")
            raise

        # Metadata for Dagster UI
        row_count = len(final_features_df)
        preview_df = final_features_df.head(10)
        
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