#!/usr/bin/env bash
# Checks only: no task execution, snapshot, source freshness query or dbt build.
set -euo pipefail

python /opt/pipeline/scripts/validate_dag.py

export PYTHONPATH=/opt/pipeline
export PYTHONDONTWRITEBYTECODE=1
export PIPELINE_BATCH_DATE=2026-11-06
export DBT_PROFILES_DIR=/opt/pipeline/profiles
export DBT_LOG_PATH=/opt/airflow/logs/pipeline/validation/dbt_logs
export DBT_TARGET_PATH=/opt/airflow/logs/pipeline/validation/target
export DBT_SEND_ANONYMOUS_USAGE_STATS=false

/opt/pipeline-venv/bin/python /opt/pipeline/scripts/validate_pipeline.py
/opt/pipeline-venv/bin/python -c 'import pandas, pyarrow, snowflake.connector; print("Pipeline imports passed.")'
/opt/pipeline-venv/bin/python -m pip check
/opt/pipeline-venv/bin/dbt deps --project-dir /opt/pipeline/dbt_project
/opt/pipeline-venv/bin/dbt parse --project-dir /opt/pipeline/dbt_project --target dev
