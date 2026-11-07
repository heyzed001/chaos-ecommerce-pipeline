"""Daily source generation, Snowflake ingestion and dbt orchestration."""

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.standard.operators.bash import BashOperator


BATCH_DATE = "{{ params.batch_date or (data_interval_start.in_timezone('Africa/Lagos').to_date_string() if data_interval_start else '') }}"
RUN_PATH = "/opt/airflow/logs/pipeline/{{ run_id | replace(':', '_') | replace('/', '_') }}/{{ task.task_id }}"

with DAG(
    dag_id="chaos_ecommerce_daily",
    description="Generate one day's data, load RAW, snapshot customers and build tested dbt marts.",
    start_date=pendulum.datetime(2026, 11, 4, 3, tz="Africa/Lagos"),
    schedule=CronDataIntervalTimetable("0 3 * * *", timezone="Africa/Lagos"),
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "aziz",
        "depends_on_past": True,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "batch_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            description="Optional YYYY-MM-DD override for manual runs. Use dates after 2026-11-03 and process oldest first.",
        )
    },
    tags=["ecommerce", "snowflake", "dbt"],
    doc_md="""
    Runs at **03:00 Africa/Lagos** for the preceding interval's calendar date.
    Catchup starts with **2026-11-04**, after the manually replayed history.
    One run is active at a time; each task waits for its previous run to succeed.
    The DAG starts paused. Review the runbook before enabling catchup.

    Manual runs should specify `batch_date`. Do not replay dates older than
    current customer snapshot versions. Retries of the current batch are safe;
    failed earlier runs must be resolved before moving forward.
    """,
) as dag:

    def step(task_id: str, command: str) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command="set -euo pipefail\n" + command,
            cwd="/opt/pipeline",
            append_env=True,
            env={
                "PATH": "/opt/pipeline-venv/bin:/usr/local/bin:/usr/bin:/bin",
                "PYTHONPATH": "/opt/pipeline",
                "PYTHONDONTWRITEBYTECODE": "1",
                "DBT_PROFILES_DIR": "/opt/pipeline/profiles",
                "DBT_LOG_PATH": RUN_PATH + "/dbt_logs",
                "DBT_TARGET_PATH": RUN_PATH + "/target",
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
                "PIPELINE_BATCH_DATE": BATCH_DATE,
                "DBT_VARS": '{{ {"customer_snapshot_date": (params.batch_date or (data_interval_start.in_timezone("Africa/Lagos").to_date_string() if data_interval_start else ""))} | tojson }}',
            },
            do_xcom_push=False,
            skip_on_exit_code=None,
        )

    validate = step("validate_configuration", "python /opt/pipeline/scripts/validate_pipeline.py")
    dependencies = step("prepare_dbt", "dbt deps --project-dir /opt/pipeline/dbt_project")
    generate = step("generate_data", 'python -m simulator.generate --date "$PIPELINE_BATCH_DATE" --out-dir /opt/pipeline/data')
    load = step("load_raw", 'python -m ingestion.load --date "$PIPELINE_BATCH_DATE" --data-dir /opt/pipeline/data')
    freshness = step("check_source_freshness", "dbt source freshness --project-dir /opt/pipeline/dbt_project --target dev")
    snapshot = step("snapshot_customers", 'dbt snapshot --project-dir /opt/pipeline/dbt_project --target dev --select snap_customers --vars "$DBT_VARS"')
    build = step("build_models", 'dbt build --project-dir /opt/pipeline/dbt_project --target dev --select staging intermediate marts --vars "$DBT_VARS"')

    validate >> dependencies >> generate >> load >> freshness >> snapshot >> build
