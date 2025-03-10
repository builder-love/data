## Data Orchestration
This project contains pipelines and connections for builder.love. 

## Architecture
Builder love uses a low-powered Google Cloud SQL Postgres database with multiple schemas as a single production database. This is sub-optimal, but aligns with the current budget. We will update the architecture to a development/production database structure once we get funding. 

Data ingestion, normalization, cleaning, and loading operations are orchestrated using Dagster and dbt. Dagster does all of the ingestion, asset management, and scheduling tasks. dbt is used to normalize, clean, and load data across the schemas. All of these scripts run outside of Google Cloud. This is also sub-optimal, but budget aligned. 

## Data taxonomy

The below tables have data appended with a timestamp on the 15th of each month:
- start by getting the complete list of project toml files from crypto ecosystems repo
    - Create project organization table from toml files
    - Create project sub ecosystems table from toml files
    - Create project repos table from toml files

The below tables are deleted and recreated with a timestamp on the 15th of each month:
- Distinct project repos table created from project repos table
- Active, distinct project repos table created from distinct project repos table

There are four schemas in the database: raw, clean, public, and api
- The raw schema is where data retrieved from across the internet is ingested
- The raw schema tables are generally appended to with a consistent data_timestamp that tracks each ingestion job
- dbt scripts run daily to create the clean schema tables
- The clean schema consists of some point in time “snapshot” tables, as well as time series data
- Most tables in the clean schema have had data processing routines and checks performed (i.e., anomalies and errors, etc.)
- Clean schema tables do not update unless something has changed in the raw source tables.
- The public schema is an exact copy of the clean schema, and is meant to be a stable environment for the builder.love platform production environment, meaning the public schema will not update if there are errors upstream
- The api schema is a series of views for the API
