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

# The runtime Compose mount supplies only a credential-free profile template.
# Actual Snowflake values are inherited by tasks from the worker's env_file.
