"""Daily source generation, Snowflake ingestion and dbt orchestration."""

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode, TestBehavior
from pipeline_alerts import notify_dag_success, notify_task_failure


BATCH_DATE = "{{ params.batch_date or (data_interval_start.in_timezone('Africa/Lagos').to_date_string() if data_interval_start else '') }}"
RUN_PATH = "/opt/airflow/logs/pipeline/{{ run_id | replace(':', '_') | replace('/', '_') }}/{{ task.task_id }}"
COUNTS_PATH = "/opt/airflow/logs/pipeline/{{ run_id | replace(':', '_') | replace('/', '_') }}/idempotency_counts.json"

with DAG(
    dag_id="chaos_ecommerce_daily",
    description="Generate one day's data, load RAW, snapshot customers and build tested dbt marts.",
    start_date=pendulum.datetime(2026, 11, 4, 10, 20, tz="Africa/Lagos"),
    schedule=CronDataIntervalTimetable("20 10 * * *", timezone="Africa/Lagos"),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=4,
    on_success_callback=notify_dag_success,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "aziz",
        "depends_on_past": True,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": notify_task_failure,
    },
    params={
        "check_idempotency": Param(default=False, type="boolean", description="Compare before/after counts for an unchanged, already loaded latest batch. Leave off for new daily data."),
        "batch_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            description="Optional YYYY-MM-DD override for manual runs. Use dates after 2026-11-03 and process oldest first.",
        )
    },
    tags=["ecommerce", "snowflake", "dbt"],
    doc_md="""
    Runs at **10:20 Africa/Lagos** for the preceding interval's calendar date.
    Catchup starts with **2026-11-04**, after the manually replayed history.
    One run is active at a time; data-processing tasks wait for their previous
    run to succeed. Optional count checks depend only on this run's upstream tasks.
    The DAG starts paused. Review the runbook before enabling catchup.

    Manual runs should specify `batch_date`. Do not replay dates older than
    current customer snapshot versions. Retries of the current batch are safe;
    failed earlier runs must be resolved before moving forward.
    """,
) as dag:

    def step(task_id: str, command: str, *, depends_on_past: bool = True) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            depends_on_past=depends_on_past,
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
                "CHECK_IDEMPOTENCY": "{{ 'true' if params.get('check_idempotency', false) else 'false' }}",
                "COUNTS_BASELINE": COUNTS_PATH,
                "DBT_VARS": '{{ {"customer_snapshot_date": (params.batch_date or (data_interval_start.in_timezone("Africa/Lagos").to_date_string() if data_interval_start else ""))} | tojson }}',
            },
            do_xcom_push=False,
            skip_on_exit_code=None,
        )

    validate = step("validate_configuration", "python /opt/pipeline/scripts/validate_pipeline.py")
    dependencies = step("prepare_dbt", "dbt deps --project-dir /opt/pipeline/dbt_project")
    capture_counts = step("capture_counts", 'if [ "$CHECK_IDEMPOTENCY" = "true" ]; then python -m ingestion.check_idempotency capture --date "$PIPELINE_BATCH_DATE" --baseline "$COUNTS_BASELINE"; else echo "Idempotency count check disabled"; fi', depends_on_past=False)
    generate = step("generate_data", 'python -m simulator.generate --date "$PIPELINE_BATCH_DATE" --out-dir /opt/pipeline/data')
    load = step("load_raw", 'python -m ingestion.load --date "$PIPELINE_BATCH_DATE" --data-dir /opt/pipeline/data')
    freshness = step("check_source_freshness", "dbt source freshness --project-dir /opt/pipeline/dbt_project --target dev")
    # Source rendering is disabled in Cosmos because freshness is handled above.
    # Keep the RAW tests that were included by the previous dbt build selection.
    raw_tests = step("test_raw_sources", 'dbt test --project-dir /opt/pipeline/dbt_project --target dev --select source:raw --indirect-selection cautious --vars "$DBT_VARS"', depends_on_past=False)
    snapshot = step("snapshot_customers", 'dbt snapshot --project-dir /opt/pipeline/dbt_project --target dev --select snap_customers --vars "$DBT_VARS"')
    build = DbtTaskGroup(
        group_id="build_models",
        project_config=ProjectConfig(
            dbt_project_path="/opt/pipeline/dbt_project",
            manifest_path="/opt/pipeline/manifests/manifest.json",
            project_name="chaos_ecommerce",
            install_dbt_deps=False,
            dbt_vars={"customer_snapshot_date": BATCH_DATE},
        ),
        profile_config=ProfileConfig(
            profile_name="chaos_ecommerce",
            target_name="dev",
            profiles_yml_filepath="/opt/pipeline/profiles/profiles.yml",
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path="/opt/pipeline-venv/bin/dbt",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/staging", "path:models/intermediate", "path:models/marts"],
            test_behavior=TestBehavior.AFTER_EACH,
            should_detach_multiple_parents_tests=True,
            emit_datasets=False,
        ),
        operator_args={
            # Bootstrap new task IDs, then require their prior scheduled success.
            "depends_on_past": True,
            "ignore_first_depends_on_past": True,
            "append_env": True,
            "env": {
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            },
            "do_xcom_push": False,
        },
    )
    # Cosmos represents ephemeral models with EmptyOperators; apply the same
    # first-run policy to these placeholders as to executable model/test tasks.
    for model_task in build:
        model_task.ignore_first_depends_on_past = True
        model_task.do_xcom_push = False
    verify_counts = step("verify_counts", 'if [ "$CHECK_IDEMPOTENCY" = "true" ]; then python -m ingestion.check_idempotency verify --date "$PIPELINE_BATCH_DATE" --baseline "$COUNTS_BASELINE"; else echo "Idempotency count check disabled"; fi', depends_on_past=False)

    validate >> dependencies >> capture_counts >> generate >> load >> freshness >> raw_tests >> snapshot >> build >> verify_counts
