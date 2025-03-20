## How do we use dbt?

dbt is used to normalize, clean, and load data between schemas and databases. 

dbt models are a good choice for cleaning and normalizing data since it is mostly standard SQL with standard database object testing built in.

Ingestion and operation orchestration is handled by dagster, which offers the advantage of a robus user interface and the flexibility of Python.
