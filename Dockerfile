FROM apache/airflow:3.1.5

ARG AIRFLOW_HOME_ARG=/opt/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME_ARG}

# Install DuckDB CLI — apt-get first (unzip/curl aren't in the base image),
# architecture-detected so this doesn't silently ship an amd64 binary into
# an arm64 build (e.g. Apple Silicon), which would only fail at task
# runtime with "exec format error", not at build time.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip \
    && ARCH=$(dpkg --print-architecture) \
    && if [ "$ARCH" = "arm64" ]; then DUCKDB_ARCH="linux-arm64"; else DUCKDB_ARCH="linux-amd64"; fi \
    && curl -L "https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-${DUCKDB_ARCH}.zip" -o /tmp/duckdb.zip \
    && unzip /tmp/duckdb.zip -d /usr/local/bin \
    && chmod +x /usr/local/bin/duckdb \
    && rm /tmp/duckdb.zip \
    && apt-get purge -y unzip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Each folder under AIRFLOW_HOME can be imported directly in Python
ENV PYTHONPATH=${AIRFLOW_HOME}:$PYTHONPATH

# requirements.txt copied and installed before the code COPY layers below,
# so a code-only change doesn't bust the pip-install cache layer.
COPY requirements.txt /
RUN pip install -r /requirements.txt

# Bake code into the image (dags, plugins, pipeline). dataSource/ and
# warehouse/ are deliberately NOT copied here — docker-compose.yml already
# bind-mounts both at runtime, and a build-time COPY would just be silently
# overlaid by that mount, wasting build time/image size for no effect.
COPY --chown=airflow:airflow dags /opt/airflow/dags
COPY --chown=airflow:airflow plugins /opt/airflow/plugins
COPY --chown=airflow:airflow pipeline /opt/airflow/pipeline

WORKDIR /opt/airflow