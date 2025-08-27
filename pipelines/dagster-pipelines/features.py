import os
import psutil
import re
import pandas as pd
import dagster as dg
from dagster import AssetKey, AssetIn
from sqlalchemy import text
import sqlalchemy
# from sentence_transformers import SentenceTransformer
import math
import numpy as np
import gc
from google.cloud import storage
import pickle
import pyarrow as pa
import pyarrow.parquet as pq

def aggregate_corpus_text(features_df: pd.DataFrame, context: dg.OpExecutionContext) -> pd.DataFrame:
    """
    Takes the raw, multi-row DataFrame and aggregates text columns into one row per repo.
    """
    context.log.info(f"Aggregating text data for {features_df['repo'].nunique()} unique repos...")

    # Handle missing data 
    text_cols_to_agg = ['repo' ,'description', 'readme_content', 'file_content']
    features_df[text_cols_to_agg] = features_df[text_cols_to_agg].fillna('')

    agg_funcs = {
        'description': 'first',
        'readme_content': 'first',
        'file_content': ' '.join
    }
    corpus_df = features_df.groupby('repo').agg(agg_funcs).reset_index()
    context.log.info(f"Text aggregation complete. Created corpus_df with {len(corpus_df)} rows.")
    return corpus_df


# def generate_embeddings(corpus_df: pd.DataFrame, context: dg.OpExecutionContext) -> pd.DataFrame:
#     """
#     Takes an aggregated DataFrame, creates a corpus, processes it in batches,
#     and returns embeddings with structured progress logging.
#     """
#     context.log.info("Preparing corpus for embedding generation...")
#     # Combine the aggregated columns into a single text document per row
#     corpus_series = (
#         corpus_df['description'] + ' ' +
#         corpus_df['readme_content'] + ' ' +
#         corpus_df['file_content']
#     ).str.replace(r'\s+', ' ', regex=True).str.strip()

#     if corpus_series.empty or not corpus_series.any():
#         context.log.warning("Corpus is empty after combining text. No embeddings will be generated.")
#         return pd.DataFrame()

#     corpus = corpus_series.tolist()
#     repo_list = corpus_df['repo'].tolist() # Get repo names for the final DataFrame

#     # Corpus is in a Python list; no longer need the DataFrame that holds the same text data. 
#     # Delete it before starting the memory-intensive encoding.
#     context.log.info(f"Dropping corpus_df from memory before starting model encoding...")
#     del corpus_df
#     del corpus_series
#     gc.collect()
#     context.log.info("Memory from corpus_df has been released.")
    
#     context.log.info(f"Successfully created a corpus with {len(corpus)} documents.")
    
#     # Load the model
#     model = SentenceTransformer('all-mpnet-base-v2')

#     # Define a batch size
#     batch_size = 128
#     all_embeddings = []

#     context.log.info(f"Starting embedding generation with batch size {batch_size}...")
    
#     total_batches = math.ceil(len(corpus) / batch_size)

#     for i in range(0, len(corpus), batch_size):
#         # Get the current batch of text
#         batch = corpus[i:i + batch_size]
        
#         # Generate embeddings for the batch. 
#         batch_embeddings = model.encode(batch, show_progress_bar=False)
#         all_embeddings.append(batch_embeddings)

#         # Log structured progress to the Dagster UI
#         current_batch_num = (i // batch_size) + 1
#         context.log.info(f"Processed batch {current_batch_num} of {total_batches}...")

#     context.log.info("Embedding generation complete. Concatenating results.")

#     # Combine the list of batch embeddings into a single numpy array
#     embeddings = np.vstack(all_embeddings)

#     # create a dataframe with the embeddings
#     embeddings_df = pd.DataFrame(embeddings, columns=[f'embedding_{i}' for i in range(embeddings.shape[1])])
    
#     # Add the repo column back for merging
#     embeddings_df['repo'] = repo_list
    
#     context.log.info(f"Finished generating embeddings. Final shape: {embeddings_df.shape}")
#     return embeddings_df

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
    "is_interview_related": ["interview", "hiring test", "hiring exercise"],
    "is_learning_material": ["learn solidity", "learning"],
    "is_mcp_server": ["mcp server"],
    "is_plugin": ["plug-in", "plugin"],
    "is_sample_project": ["sample", "sample application", "sample project", "simple example", "Sample Hardhat Project", "advanced sample hardhat project"],
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
    "is_research": ["research", "research paper", "research project", "research related", "researching", "researching project", "research project related", "Bachelor thesis", "Master thesis", "PhD thesis", "thesis project", "thesis related", "thesis researching", "thesis project related"],
    "has_mainnet": ["mainnet"],
    "has_testnet": ["testnet"],
    "is_blockchain": ["blockchain"],
    "is_monorepo": ["monorepo"],
}

REPO_NAME_FEATURE_MAP = {
    "name_is_example": ["example"],
    "name_is_demo": ["demo"],
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
    "name_is_interview": ["interview", "task", "exercise"],
    "name_is_monorepo": ["monorepo"],
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
        "pm_has_repository": ['"repository":', '"repositorie":', '"repo":', '"repos":'],
        "pm_has_fontend_dependencies": [
            # UI Frameworks/Libraries (main packages and common sub-packages)
            '"vue":', '"react":', '"svelte":', '"preact":', '"lit"', 
            '"react-dom"', 
            '"@angular/core"', '"@angular/common"', '"@angular/platform-browser"', 
            '"vue-loader"', '"svelte-loader"', '"react-router-dom"', '"redux"', 
            '"react-query"', '"swr"', 

            # Meta-Frameworks / SSR
            '"next":', '"nuxt":', '"sveltekit":', '"gatsby":', '"remix":', 
            '"@remix-run/react"', '"@remix-run/node"', # For Remix

            # Styling/Component Libraries
            '"tailwindcss":', '"styled-components"', '"@emotion/react"', '"@mui/material"', 
            '"bootstrap":', '"antd":', '"chakra-ui"', '"primevue"', '"element-plus"',

            # Build Tools (often appear in devDependencies)
            '"vite":', '"webpack":', '"rollup":', '"parcel":', '"esbuild":', '"snowpack":'
        ],
        "pm_has_backend_dependencies": ['"express":', '"fastify":', '"nestjs":', '"nest":', '"nestjs":', '"koa"', '"fastify"', '"hapi"', '"sails"'],
        "pm_has_css_preprocessors": ['"sass"', '"less"', '"stylus"'],
        "pm_has_bundler": ['"webpack"', '"vite"', '"rollup"', '"parcel"', '"esbuild"'],
        "pm_has_database_client": ['"pg"', '"mysql"', '"mysql2"', '"sqlite3"', '"mongoose"', '"sequelize"', '"typeorm"'],
        "pm_is_cli_tool": ['"commander"', '"yargs"', '"inquirer"', '"oclif"'],
        "pm_is_library_project": ['"main":', '"module":', '"types":', '"exports":'],
    },
    "Cargo.toml": { # Rust (TOML)
        "pm_has_main_entrypoint": ['[lib]'],
        "pm_has_bin_script": ['[[bin]]'],
        "pm_has_dependencies": ['[dependencies]'],
        "pm_has_version_control": ['version ='], # Typically under [package]
        "pm_has_author_cited": ['authors ='],
        "pm_has_license": ['license ='],
        "pm_has_repository": ['repository ='],
        "pm_has_web_framework": ["actix-web", "rocket", "axum", "warp", "tokio"],
        "pm_has_database_driver": ["sqlx", "diesel", "postgres", "mysql", "redis"],
        "pm_is_blockchain_infra": ["substrate", "solana-sdk", "ethers", "web3"],
        "pm_is_cli_tool": ["clap", "structopt"],
    },
    "pom.xml": { # Java Maven (XML)
        "pm_has_main_entrypoint": ['<mainClass>'],
        "pm_has_bin_script": ['maven-assembly-plugin', 'maven-shade-plugin'], # Heuristic for executable jars
        "pm_has_dependencies": ['<dependencies>'],
        "pm_has_version_control": ['<version>'], # This tag is used for both project and dependency versions
        "pm_has_author_cited": ['<developer>', '<contributor>'],
        "pm_has_license": ['<licenses>'],
        "pm_has_repository": ['<scm>'], # Source Control Management
    },
    "pyproject.toml": { # Python (TOML)
        "pm_has_main_entrypoint": ['[project.scripts]', '[project.gui-scripts]'],
        "pm_has_bin_script": ['[project.scripts]', '[project.gui-scripts]'],
        "pm_has_dependencies": ['[project.dependencies]', '[tool.poetry.dependencies]'],
        "pm_has_version_control": ['version ='], # Under [project] or [tool.poetry]
        "pm_has_author_cited": ['authors =', '[project.authors]'],
        "pm_has_license": ['license ='],
        "pm_has_repository": ['[project.urls]', 'repository =', 'homepage ='],
        "pm_has_web_framework": ["django", "flask", "fastapi", "sanic", "tornado"],
        "pm_has_database_orm": ["sqlalchemy", "django.db", "peewee", "tortoise-orm"],
        "pm_is_devops_tool": ["ansible", "fabric", "pyinfra"],
        "pm_is_testing_framework": ["pytest", "unittest", "behave", "robotframework"],
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
        "pm_has_repository": ['git_source', 'github'],
    },
    # This captures metadata for RubyGems
    ".gemspec": {
        "pm_has_main_entrypoint": ['spec.require_paths'],
        "pm_has_bin_script": ['spec.executables'],
        "pm_has_dependencies": ['spec.add_dependency'],
        "pm_has_version_control": ['spec.version'],
        "pm_has_author_cited": ['spec.authors'],
        "pm_has_license": ['spec.license'],
        "pm_has_repository": ['spec.homepage', "spec.metadata['source_code_uri']"],
    },
    "build.gradle": { # Groovy Gradle
        "pm_has_main_entrypoint": ["mainClassName"],
        "pm_has_bin_script": ["application"], # The application plugin creates start scripts
        "pm_has_dependencies": ["dependencies {", "implementation ", "api "],
        "pm_has_version_control": ["version ="],
        "pm_has_author_cited": [], # Not a standard field
        "pm_has_license": ["licenses {"], # From publishing plugin
        "pm_has_repository": ["scm {"], # From publishing plugin
    },
    "build.gradle.kts": { # Kotlin Gradle
        "pm_has_main_entrypoint": ["mainClass.set("],
        "pm_has_bin_script": ["application"],
        "pm_has_dependencies": ["dependencies {", "implementation("],
        "pm_has_version_control": ["version ="],
        "pm_has_author_cited": [],
        "pm_has_license": ["licenses {"],
        "pm_has_repository": ["scm {"],
    },
    "go.mod": { # Go
        "pm_has_main_entrypoint": [], # Not specified in go.mod
        "pm_has_bin_script": [], # Not specified in go.mod
        "pm_has_dependencies": ["require (", "require "],
        "pm_has_version_control": [], # Not specified in go.mod
        "pm_has_author_cited": [], # Not specified in go.mod
        "pm_has_license": [], # Not specified in go.mod
        "pm_has_repository": ["module "], # The module path is the repository,
        "pm_has_web_framework": ["net/http", "gin-gonic", "gorilla/mux", "chi", "echo"],
        "pm_has_database_driver": ["database/sql", "gorm", "sqlx"],
        "pm_is_devops_infra": ["kubernetes", "docker", "moby", "prometheus", "hashicorp/terraform"],
        "pm_is_blockchain_infra": ["etcd", "tendermint", "cosmos-sdk", "go-ethereum"]
    },
    "setup.py": { # Python Legacy
        "pm_has_main_entrypoint": ["entry_points"],
        "pm_has_bin_script": ["console_scripts"],
        "pm_has_dependencies": ["install_requires"],
        "pm_has_version_control": ["version="],
        "pm_has_author_cited": ["author="],
        "pm_has_license": ["license="],
        "pm_has_repository": ["url="],
    },
     "setup.cfg": { # Python Legacy
        "pm_has_main_entrypoint": ["entry_points"],
        "pm_has_bin_script": ["console_scripts"],
        "pm_has_dependencies": ["install_requires"],
        "pm_has_version_control": ["version ="],
        "pm_has_author_cited": ["author ="],
        "pm_has_license": ["license ="],
        "pm_has_repository": ["url ="],
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
        raw_schema = env_config["raw_schema"]

        active_repos_table = "latest_active_distinct_project_repos_with_code"
        description_table = "latest_project_repos_description"
        readme_table = "latest_project_repos_readmes"
        package_files_table = "latest_project_repos_package_files"
        is_fork_table = "latest_project_repos_is_fork"
        dominant_language_table = "latest_project_repos_dominant_language"
        output_table_name = "latest_project_repos_features"

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
                        f.is_fork,
                        l.dominant_language
                    FROM {clean_schema}.{active_repos_table} AS a
                    LEFT JOIN {clean_schema}.{description_table} AS d ON a.repo = d.repo
                    LEFT JOIN {clean_schema}.{readme_table} AS r ON a.repo = r.repo
                    LEFT JOIN {clean_schema}.{package_files_table} AS p ON a.repo = p.repo
                    LEFT JOIN {clean_schema}.{is_fork_table} AS f ON a.repo = f.repo 
                    LEFT JOIN {clean_schema}.{dominant_language_table} AS l ON a.repo = l.repo
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
        features_df['dominant_language'].fillna('', inplace=True)

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

        # initialize the readme is single header boolean
        features_df['readme_is_single_header'] = False
        
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
            # Create one search text column, then search once
            search_text = features_df['description'] + ' ' + features_df['readme_content']
            features_df[feature_name] = search_text.str.contains(pattern, case=False, regex=True)
        context.log.info(f"Generated {len(KEYWORD_FEATURE_MAP)} boolean feature columns from description and readme.")

        # Generate the new single-header readme feature ---
        context.log.info("Checking for READMEs that only contain a single header...")
        # A vectorized approach for performance:
        # 1. Strip all leading/trailing whitespace from the readme content.
        # 2. Check for three conditions:
        #    a. The content is not empty.
        #    b. The content does NOT contain any newline characters (it's a single line).
        #    c. The content starts with a '#' character.
        stripped_readme = features_df['readme_content'].str.strip()
        is_not_empty = stripped_readme != ''
        is_single_line = ~stripped_readme.str.contains('\n', na=False)
        starts_with_hash = stripped_readme.str.startswith('#', na=False)
        
        features_df['readme_is_single_header'] = is_not_empty & is_single_line & starts_with_hash
        context.log.info(f"Identified {features_df['readme_is_single_header'].sum()} repos with single-header READMEs.")

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
        agg_funcs = { 'data_timestamp': 'first', 'is_fork': 'first', 'dominant_language': 'first' }
        feature_and_flag_cols = list(all_feature_names) + ['has_readme', 'has_description', 'has_package_file', 'readme_is_single_header']
        for col in feature_and_flag_cols:
             agg_funcs[col] = 'any'
        
        # Select only the columns needed for aggregation
        cols_to_agg = ['repo', 'data_timestamp', 'is_fork', 'dominant_language'] + feature_and_flag_cols
        final_features_df = features_df[cols_to_agg].groupby('repo').agg(agg_funcs).reset_index()

        context.log.info(f"Aggregation complete. Result has {len(final_features_df)} unique repos.")

        # # generate semantic embeddings
        # try:
        #     context.log.info("Aggregating corpus text...")
        #     corpus_df = aggregate_corpus_text(features_df, context) # Use the original raw df

        #     # The raw features_df is now redundant. Delete it to free up memory.
        #     context.log.info(f"Dropping raw features_df from memory to conserve resources...")
        #     del features_df
        #     gc.collect() # Ask the garbage collector to free up the memory now
        #     context.log.info("Memory from raw features_df has been released.")

        #     context.log.info("Generating semantic embeddings...")
        #     embeddings_df = generate_embeddings(corpus_df, context)
        # except Exception as e:
        #     context.log.error(f"Failed during embedding generation: {e}")
        #     raise

        # # merge all features together
        # if embeddings_df.empty:
        #     context.log.info("No embeddings generated. No features to merge.")
        #     raise

        # context.log.info("Merging keyword features with semantic embeddings...")
        # final_features_df = pd.merge(final_features_df, embeddings_df, on='repo', how='left')

        # # Fill any NaNs created if a repo had no text for embeddings
        # embedding_cols = [col for col in final_features_df if col.startswith('embedding_')]
        # final_features_df[embedding_cols] = final_features_df[embedding_cols].fillna(0)
        # context.log.info(f"Merged {len(embedding_cols)} embedding columns into final_features_df.")
        
        # context.log.info(f"Final combined feature set created with {len(final_features_df)} rows.")

        # Define dtypes for the output table
        output_dtype_mapping = {
            'repo': sqlalchemy.types.Text,
            'data_timestamp': sqlalchemy.types.TIMESTAMP(timezone=False),
        }
        for col in final_features_df.columns:
            if col in output_dtype_mapping:
                continue # Skip already defined columns

            # Handle boolean features
            if final_features_df[col].dtype == 'bool':
                output_dtype_mapping[col] = sqlalchemy.types.Boolean
            # Handle embedding features
            elif pd.api.types.is_float_dtype(final_features_df[col]):
                # Use FLOAT or REAL for efficiency over NUMERIC/DECIMAL
                output_dtype_mapping[col] = sqlalchemy.types.FLOAT

        # Write the augmented DataFrame to the new table
        try:
            # perform database operations in a transaction
            with cloud_sql_engine.begin() as conn:
                # replace existing table
                final_features_df.to_sql(
                    output_table_name,
                    conn,
                    schema=raw_schema,
                    if_exists='replace',
                    index=False,
                    dtype=output_dtype_mapping
                )
                context.log.info(f"Successfully wrote {len(final_features_df)} rows with features to {raw_schema}.{output_table_name}")

                # add placeholder columns for prediction fields
                conn.execute(text(f"ALTER TABLE {raw_schema}.{output_table_name} ADD COLUMN predicted_is_scaffold boolean"))
                conn.execute(text(f"ALTER TABLE {raw_schema}.{output_table_name} ADD COLUMN predicted_is_educational boolean"))
                conn.execute(text(f"ALTER TABLE {raw_schema}.{output_table_name} ADD COLUMN predicted_is_dev_tooling boolean"))
                conn.execute(text(f"ALTER TABLE {raw_schema}.{output_table_name} ADD COLUMN predicted_is_app boolean"))
                conn.execute(text(f"ALTER TABLE {raw_schema}.{output_table_name} ADD COLUMN predicted_is_infrastructure boolean"))

        except Exception as e:
            context.log.error(f"Failed to write features to {raw_schema}.{output_table_name}: {e}")
            raise

        # Metadata for Dagster UI
        row_count = len(final_features_df)
        preview_df = final_features_df.head(10)
        
        preview_columns = ['repo', 'has_readme'] + list(KEYWORD_FEATURE_MAP.keys())[:4]
        actual_preview_columns = [col for col in preview_columns if col in preview_df.columns]

        # get count of columns in the final_features_df
        num_features_generated = len(final_features_df.columns) - 10 # subtract 10 for the non-feature columns

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "num_features_generated": dg.MetadataValue.int(num_features_generated),
                "output_table": dg.MetadataValue.text(f"{raw_schema}.{output_table_name}"),
                "preview": dg.MetadataValue.md(preview_df[actual_preview_columns].to_markdown(index=False))
            }
        )

    return _project_repos_description_features_env_specific


# create corpus text and pass to gcs bucket
# factory function
def create_project_repos_corpus_asset(env_prefix: str):
    """
    Factory function to create an asset that generates corpus from repo files and loads to gcs bucket.
    The primary sources are repo descriptions, readmes, doc site .md files, and package manager files.
    """

    @dg.asset(
        key_prefix=env_prefix,
        name="project_repos_corpus",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "gcs"},
        group_name="feature_engineering",
        description="Generates corpus from repo files and loads to gcs bucket.",
        tags={"feature_engineering": "True"}
    )
    def _project_repos_corpus_env_specific(context: dg.OpExecutionContext) -> dg.MaterializeResult:
        process = psutil.Process(os.getpid())
        def log_memory_usage(stage: str):
            memory_mb = process.memory_info().rss / (1024 * 1024)
            context.log.info(f"Memory Usage ({stage}): {memory_mb:.2f} MB")

        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        env_config = context.resources.active_env_config
        clean_schema = env_config["clean_schema"]
        raw_schema = env_config["raw_schema"]

        # Cloud storage info
        bucket_name = "bl-repo-corpus-public"
        gcs_client = context.resources.gcs
        bucket = gcs_client.bucket(bucket_name)

        # Cloud sql info
        active_repos_table = "latest_active_distinct_project_repos_with_code"
        description_table = "latest_project_repos_description"
        readme_table = "latest_project_repos_readmes"
        package_files_table = "latest_project_repos_package_files"
        is_fork_table = "latest_project_repos_is_fork"
        dominant_language_table = "latest_project_repos_dominant_language"
        doc_site_table = "latest_project_repos_documentation_files"
        framework_files_table = "latest_project_repos_framework_files"
        
        sql_query_string = f"""
        WITH repo_data AS (
            SELECT
                a.repo,
                MAX(d.description) as description,
                MAX(r.readme_content) as readme_content,
                STRING_AGG(p.file_content, ' ') as file_content,
                STRING_AGG(ff.file_content, ' ') as framework_file_content,
                STRING_AGG(ds.file_content, ' ') as doc_site_file_content,
                BOOL_OR(f.is_fork) as is_fork,
                MAX(l.dominant_language) as dominant_language
            FROM {clean_schema}.{active_repos_table} AS a
            LEFT JOIN {clean_schema}.{description_table} AS d ON a.repo = d.repo
            LEFT JOIN {clean_schema}.{readme_table} AS r ON a.repo = r.repo
            LEFT JOIN {clean_schema}.{package_files_table} AS p ON a.repo = p.repo
            LEFT JOIN {clean_schema}.{is_fork_table} AS f ON a.repo = f.repo 
            LEFT JOIN {clean_schema}.{dominant_language_table} AS l ON a.repo = l.repo
            LEFT JOIN {raw_schema}.{doc_site_table} AS ds ON a.repo = ds.repo
            LEFT JOIN {raw_schema}.{framework_files_table} AS ff ON a.repo = ff.repo
            GROUP BY a.repo
        )
        SELECT 
            repo,
            CONCAT_WS(' ',
                description,
                readme_content,
                file_content,
                framework_file_content,
                doc_site_file_content,
                CASE WHEN is_fork THEN 'this repo is a fork' END,
                CASE WHEN dominant_language is not null THEN 'the dominant language of the repo is ' || dominant_language END
            ) AS corpus_text
        FROM repo_data
        WHERE description is not null or readme_content is not null or file_content is not null
        """

        context.log.info("Starting to process and stream data in chunks directly to GCS.")
        log_memory_usage("Before Query")
        
        total_rows = 0
        writer = None
        
        blob_name = f"embeddings_data/{context.run_id}.parquet"
        blob = bucket.blob(blob_name)
        context.log.info(f"Streaming Parquet file to gs://{bucket_name}/{blob_name}")

        db_connection = cloud_sql_engine.connect().execution_options(stream_results=True)

        try:
            with blob.open('wb') as gcs_writer_stream:
                with db_connection.connection.cursor('repo_cursor') as cursor:
                    cursor.execute(sql_query_string)
                    
                    while True:
                        rows = cursor.fetchmany(1000)
                        if not rows:
                            break

                        chunk_df = pd.DataFrame(rows, columns=['repo', 'corpus_text'])
                        
                        log_memory_usage(f"After loading chunk for {total_rows + len(chunk_df)} rows")
                        chunk_size_mb = chunk_df.memory_usage(deep=True).sum() / (1024*1024)
                        context.log.info(f"Processing chunk with {len(chunk_df)} rows. Pandas chunk in-memory size: {chunk_size_mb:.2f} MB")
                        
                        total_rows += len(chunk_df)
                        table = pa.Table.from_pandas(chunk_df, preserve_index=False)

                        if writer is None:
                            writer = pq.ParquetWriter(gcs_writer_stream, table.schema)
                        
                        writer.write_table(table)

                # Close the writer inside the `with blob.open` block
                if writer:
                    writer.close()
                    context.log.info("Parquet writer closed successfully.")
            
            if total_rows > 0:
                 context.log.info(f"Finished processing and streaming {total_rows} rows.")
            else:
                context.log.warning("No data found to process.")
                return dg.MaterializeResult(metadata={"row_count": 0})

        finally:
            db_connection.close()
    
        return dg.MaterializeResult(
            metadata={
                "gcs_path": dg.MetadataValue.text(f"gs://{bucket_name}/{blob_name}"),
                "num_rows": dg.MetadataValue.int(total_rows)
            }
        )

    return _project_repos_corpus_env_specific


# factory function to get embeddings from gcs bucket and store them in postgres vector column - table raw.repo_corpus_embeddings
def create_project_repos_corpus_embeddings_asset(env_prefix: str):
    """
    Factory function to create an asset that gets embeddings from gcs bucket and stores them in postgres vector column - table raw.repo_corpus_embeddings
    """

    @dg.asset(
        key_prefix=env_prefix,
        name="project_repos_corpus_embeddings",
        required_resource_keys={"cloud_sql_postgres_resource", "active_env_config", "gcs"},
        group_name="feature_engineering",
        description="Gets embeddings from gcs bucket and stores them in postgres vector column - table raw.repo_corpus_embeddings",
        tags={"feature_engineering": "True"}
    )
    def _project_repos_corpus_embeddings_env_specific(context: dg.OpExecutionContext) -> dg.MaterializeResult:
        cloud_sql_engine = context.resources.cloud_sql_postgres_resource
        gcs_resource = context.resources.gcs 
        storage_client = gcs_resource.get_client()
        env_config = context.resources.active_env_config
        
        # Updated configuration to point to the new Parquet files
        gcs_bucket_name = "bl-repo-corpus-public"
        # chore: get this value from upstream dagster asset's yielded output
        gcs_parquet_folder_path = "embeddings_data/akash-qwen-checkpoints/20250827-162519"
        raw_schema = env_config["raw_schema"]
        table_name = "latest_project_repo_corpus_embeddings"
        full_table_name = f"{raw_schema}.{table_name}"
        
        total_records_processed = 0

        try:
            # 1. List all the batch files in the GCS directory
            context.log.info(f"Listing batch files from gs://{gcs_bucket_name}/{gcs_parquet_folder_path}...")
            bucket = storage_client.bucket(gcs_bucket_name)
            blobs = list(bucket.list_blobs(prefix=gcs_parquet_folder_path))
            
            # Filter out the directory placeholder itself
            parquet_blobs = [b for b in blobs if b.name.endswith('.parquet')]
            context.log.info(f"Found {len(parquet_blobs)} batch files to process.")

            if not parquet_blobs:
                context.log.warning("No Parquet files found. Exiting.")
                return dg.MaterializeResult(metadata={"records_processed": 0})

            # 2. Truncate the target table ONCE before starting the loop
            with cloud_sql_engine.begin() as conn:
                context.log.info(f"Resetting target table: TRUNCATE TABLE {full_table_name};")
                conn.execute(text(f"TRUNCATE TABLE {full_table_name};"))
            
            # 3. Process each batch file one by one
            for i, blob in enumerate(parquet_blobs):
                context.log.info(f"--- Processing Batch {i+1}/{len(parquet_blobs)}: {blob.name} ---")

                # Download batch to a temporary in-memory buffer
                gcs_path = f"gs://{gcs_bucket_name}/{blob.name}"
                
                # Read the parquet file directly from GCS into a pandas DataFrame
                df = pd.read_parquet(gcs_path, filesystem=gcsfs.GCSFileSystem())
                context.log.info(f"Loaded {len(df)} records from batch.")

                # The embedding is already a list/array in the source parquet, so it should be fine.
                # If it's a numpy array, convert to list string for pgvector.
                if not df.empty and not isinstance(df['corpus_embedding'].iloc[0], str):
                    df['corpus_embedding'] = df['corpus_embedding'].apply(lambda x: str(list(x)))

                # Append the DataFrame batch to the Postgres table
                df.to_sql(
                    name=table_name,
                    con=cloud_sql_engine,
                    schema=raw_schema,
                    if_exists='append',
                    index=False,
                    chunksize=10000, # Further chunking for the database insert
                    method='multi'
                )
                total_records_processed += len(df)
                context.log.info(f"Successfully inserted batch. Total records processed: {total_records_processed}")

        except Exception as e:
            context.log.error(f"Failed during streaming refresh of {full_table_name}: {e}")
            raise

        context.log.info(f"✅ Successfully refreshed {full_table_name} with {total_records_processed} records.")
        
        return dg.MaterializeResult(
            metadata={
                "records_processed": total_records_processed,
                "batches": len(parquet_blobs),
                "gcs_source_path": f"gs://{gcs_bucket_name}/{gcs_parquet_folder_path}"
            }
        )

    return _project_repos_corpus_embeddings_env_specific





# create corpus text from the latest_active_distinct_project_repos_with_code table
# concatenate repo name, description, readme, config file content
# generate statements about the repo that we think are true, and then concatenate them into a single string
# e.g. "this repo is a scaffold for a new project", "this repo is a dev tooling repo", "this repo is an app"

