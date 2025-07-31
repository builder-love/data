# Dockerfile (for Networked Build)

# --- Builder Stage ---
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

COPY .env .

# Change to the dbt project directory
WORKDIR /opt/dagster/app/dbt-pipelines

# Use the build arguments to compile dbt.
# We set them as environment variables for this single RUN command.
RUN set -a && . ../.env && set +a && \
    dbt deps 
RUN set -a && . ../.env && set +a && \ 
    dbt debug 
RUN set -a && . ../.env && set +a && \ 
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