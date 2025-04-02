from setuptools import find_packages, setup

setup(
    name="dagster_pipelines",
    packages=find_packages(exclude=["dagster_pipelines_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster_dbt",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
