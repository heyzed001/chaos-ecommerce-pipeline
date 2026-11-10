FROM apache/airflow:3.2.2-python3.12

# Keep dbt/Snowflake dependencies out of Airflow's own Python installation.
USER root
RUN mkdir -p /opt/pipeline-venv /opt/pipeline/profiles \
    && chown -R airflow:root /opt/pipeline-venv /opt/pipeline

USER airflow
COPY requirements.txt /tmp/pipeline-requirements.txt
RUN python -m venv /opt/pipeline-venv \
    && PIP_CONSTRAINT= /opt/pipeline-venv/bin/python -m pip install --no-user --no-cache-dir -r /tmp/pipeline-requirements.txt \
    && /opt/pipeline-venv/bin/python -m pip check

# Snowflake's bundled HTTP client requires chardet below 6. Keep this fix in
# the isolated pipeline environment; do not alter Airflow's HTTP dependencies.
RUN PIP_CONSTRAINT= /opt/pipeline-venv/bin/python -m pip install --no-user --no-cache-dir 'chardet>=3.0.2,<6' \
    && /opt/pipeline-venv/bin/python -m pip check

COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/airflow-requirements.txt \
    && pip check

# The runtime Compose mount supplies only a credential-free profile template.
# Actual Snowflake values are inherited by tasks from the worker's env_file.

# Render Cosmos from a credential-free manifest, without querying Snowflake or
# exposing worker credentials to the DAG parser. Rebuild after dbt graph changes.
COPY --chown=airflow:root dbt_project /opt/pipeline/dbt_project
RUN mkdir -p /opt/pipeline/manifests /tmp/manifest-profile \
    && cp /opt/pipeline/dbt_project/profiles.yml.example /tmp/manifest-profile/profiles.yml \
    && /opt/pipeline-venv/bin/dbt deps --project-dir /opt/pipeline/dbt_project \
    && SNOWFLAKE_ACCOUNT=manifest-build SNOWFLAKE_USER=manifest-build \
       SNOWFLAKE_PASSWORD=unused-build-placeholder SNOWFLAKE_WAREHOUSE=manifest-build \
       SNOWFLAKE_DATABASE=ECOMMERCE \
       /opt/pipeline-venv/bin/dbt parse --project-dir /opt/pipeline/dbt_project \
       --profiles-dir /tmp/manifest-profile --target dev \
    && cp /opt/pipeline/dbt_project/target/manifest.json /opt/pipeline/manifests/manifest.json \
    && rm -rf /tmp/manifest-profile /opt/pipeline/dbt_project/target /opt/pipeline/dbt_project/logs
