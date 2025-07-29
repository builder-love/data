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
    
    # Change to the dbt project directory
    WORKDIR /opt/dagster/app/dbt-pipelines
    
    # Securely mount the secrets file, source the variables, and run dbt
    RUN --mount=type=secret,id=env.list,target=/run/secrets/env.list \
    echo "Forcing dbt to re-compile to see logs. V3..." && \
    set -a && . /run/secrets/env.list && set +a && \
    dbt deps && \
    dbt compile --threads 16 --target stg --target-path "target/stg" --profiles-dir . && \
    dbt compile --threads 16 --target prod --target-path "target/prod" --profiles-dir .
    
    
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
    COPY --from=builder --chown=appuser:app /opt/dagster/app .
    
    EXPOSE 3000
    
    # NO CMD HERE - The command will be specified at runtime.