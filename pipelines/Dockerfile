# Dockerfile (for Networked Build)

# --- Builder Stage ---
# Declare the arguments that will be passed in from the 'docker build' command.
ARG DB_HOST
ARG DB_NAME
ARG DB_USER
ARG DB_PASSWORD

FROM python:3.13-slim AS builder

# Install build-time dependencies
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

# Install Python packages
RUN pip install uv
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# Copy all application code
COPY . .

# Change to the dbt project directory
WORKDIR /opt/dagster/app/dbt-pipelines

# Use the build arguments to compile dbt.
# We set them as environment variables for this single RUN command.
RUN export DBT_PROFILES_DIR=. && \
    export cloud_sql_postgres_host=$DB_HOST && \
    export cloud_sql_postgres_db=$DB_NAME && \
    export cloud_sql_user=$DB_USER && \
    export cloud_sql_password=$DB_PASSWORD && \
    dbt deps && \
    dbt debug && \
    dbt compile --threads 16 --target stg --target-path "target/stg" && \
    dbt compile --threads 16 --target prod --target-path "target/prod"

# --- Final Stage ---
FROM python:3.13-slim

# Create a non-root user and group
RUN groupadd --system app && useradd --system --gid app appuser

WORKDIR /opt/dagster/app
RUN chown -R appuser:app /opt/dagster/app

USER appuser

# Copy installed Python packages AND THE EXECUTABLES from the builder stage
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
# Copy the application code, which now includes the compiled dbt models
COPY --from=builder --chown=appuser:app /opt/dagster/app .

EXPOSE 3000