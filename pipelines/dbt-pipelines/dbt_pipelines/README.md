## dbt integration

This project uses dbt models to transform and load ingested data. View the existing transform and load operations in the /models folder. dbt models are scheduled using dagster. 

dbt is used to normalize, clean, and load data to downstream environments of raw ingestion. These tasks are not exclusively handled by dbt, but where dbt offers a better tool for the job it has been used. 
