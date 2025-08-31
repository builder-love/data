# Dockerfile (for Networked Build)

# --- Builder Stage ---
FROM python:3.13-slim AS builder

# Install build-time dependencies
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y dnsutils iputils-ping iproute2

# Run all the debugging commands
# RUN echo "--- 1. Network Interfaces ---" && \
#     ip addr show

# RUN echo "\n--- 2. DNS Configuration ---" && \
#     cat /etc/resolv.conf

# RUN echo "\n--- 3. Verbose DNS Trace (`dig`) ---" && \
#     dig proxy-service

# RUN echo "\n--- 4. Standard Hostname Resolution (`nslookup`) ---" && \
#     nslookup proxy-service

WORKDIR /opt/dagster/app

# Install Python packages
RUN pip install uv
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# Copy all application code
COPY . .

# Create DAGSTER_HOME and copy the instance config
RUN mkdir -p /opt/dagster/dagster_home
# COPY dagster.yaml /opt/dagster/dagster_home/

# Change to the dbt project directory
WORKDIR /opt/dagster/app/dbt-pipelines

# Use the build arguments to compile dbt.
# We set them as environment variables for this single RUN command.
RUN set -a && . ../.env && set +a && \
    dbt deps 
RUN set -a && . ../.env && set +a && \ 
    dbt debug 
RUN set -a && . ../.env && set +a && \
    if [ "$DAGSTER_ENV" = "prod" ]; then \
        echo "--- Compiling for PRODUCTION ---" && \
        dbt compile --threads 16 --target prod --target-path "target/prod"; \
    elif [ "$DAGSTER_ENV" = "stg" ]; then \
        echo "--- Compiling for STAGING ---" && \
        dbt compile --threads 16 --target stg --target-path "target/stg"; \
    else \
        echo "--- Cannot determine environment. Compiling for STAGING ---" && \
        dbt compile --threads 16 --target stg --target-path "target/stg"; \
    fi

# After dbt compile is finished, remove the .env file so it's not copied to the final image.
RUN rm ../.env

# --- Final Stage ---
FROM python:3.13-slim

# Create a non-root user and group
RUN groupadd --system app && useradd --system --gid app appuser

# Install git, xz-utils, AND the gcloud SDK for gsutil
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    xz-utils \
    apt-transport-https \
    ca-certificates \
    gnupg \
    google-cloud-sdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# The archive will be copied from GCS now
ARG GCS_BUCKET_NAME
RUN gsutil cp gs://bl-build-artifacts/zig-linux-x86_64-0.14.0.tar.xz . && \
    tar -xf zig-linux-x86_64-0.14.0.tar.xz && \
    mv zig-linux-x86_64-0.14.0 /usr/local/lib/zig && \
    ln -s /usr/local/lib/zig/zig /usr/local/bin/zig && \
    rm zig-linux-x86_64-0.14.0.tar.xz

WORKDIR /opt/dagster/app

# Point the Zig cache to a writable temporary directory
ENV ZIG_GLOBAL_CACHE_DIR=/tmp/zig-cache

USER appuser

# Copy installed Python packages AND THE EXECUTABLES from the builder stage
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
# Copy the application code, which now includes the compiled dbt models
COPY --from=builder --chown=appuser:app /opt/dagster /opt/dagster
# explicitly copy the generated dbt artifacts on top of the source code
COPY --from=builder --chown=appuser:app /opt/dagster/app/dbt-pipelines/target /opt/dagster/app/dbt-pipelines/target

EXPOSE 3000

CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000"]