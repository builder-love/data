from setuptools import find_packages, setup

setup(
    name="dagster-pipelines",
    packages=find_packages(exclude=["dagster_pipelines_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster_dbt",
        "pandas",
        "psycopg2-binary",
        "sqlalchemy",
        "scikit-learn",
        "sentence_transformers",
        "gcsfs"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
