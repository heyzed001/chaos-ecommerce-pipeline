# Local Airflow orchestration

Optional SMTP success/failure emails and Slack failure notifications are documented in
`ALERTS.md`. Both channels are disabled until configured in `airflow/.env`.

Airflow 3.2.2 runs in Docker with Celery, Redis and PostgreSQL. The custom image
uses Python 3.12 and installs this project's requirements into a separate
`/opt/pipeline-venv`. Pipeline dependencies stay separate from Airflow. Do not mount
the Windows virtual environment into a Linux container.

Cosmos 1.16.0 is pinned in `airflow/requirements.txt` and installed in Airflow's
Python. The image pins Airflow itself to 3.2.2 and checks both environments for
dependency conflicts. The image tag is
`chaos-airflow:3.2.2-python3.12-cosmos1.16.0`.

## DAG and data dates

`dags/chaos_ecommerce_daily.py` defines this chain:

```text
validate_configuration -> prepare_dbt -> capture_counts -> generate_data -> load_raw
    -> check_source_freshness -> test_raw_sources -> snapshot_customers
    -> build_models (Cosmos task group) -> verify_counts
```

Expand **build_models** in the Airflow graph to see each dbt model and its tests.
Cosmos uses the existing dbt executable through LOCAL/SUBPROCESS execution;
dbt is not installed into Airflow's Python. All 14 models are represented,
including no-op placeholders for the two ephemeral models whose tests still run.
Multi-parent tests run after their model dependencies. RAW-source tests run in
`test_raw_sources` with cautious indirect selection, preserving their coverage
without repeating the existing freshness command. The snapshot remains a
separate, date-controlled task before transformations. dbt's incremental MERGE
configuration and snapshot logic are unchanged.

The DAG runs one batch at a time with at most four concurrent tasks. Model/test
tasks inherit retries and failure callbacks, and the final count check waits for
the whole group. The DAG success email therefore waits for all model/tests and
the optional count verification. Cosmos task IDs explicitly allow their first
execution without historical task instances, then require their previous
scheduled execution to succeed. Do not clear/replay old completed runs that
used the former single `build_models` task; trigger a new run for the latest batch.

Cosmos loads `/opt/pipeline/manifests/manifest.json`, generated during the image
build using placeholder Snowflake settings and `dbt parse` (no warehouse queries).
This keeps Snowflake credentials on the worker and avoids live `dbt ls` during
DAG parsing. **Rebuild and recreate services after changing dbt SQL, YAML,
dependencies or macros**, so the rendered graph matches the mounted project:

```powershell
# From airflow/, after active runs finish:
docker compose build
docker compose up -d --no-build
docker compose exec airflow-dag-processor python /opt/pipeline/scripts/validate_dag.py
```

If `AIRFLOW_IMAGE_NAME` is customized in your local env file, ensure it points
to the Cosmos-enabled image. Dependency preparation still runs once per batch.
Individual Cosmos tasks use separate temporary working directories; inspect
their dbt output in each Airflow task log. Cosmos manages its temporary target
and log paths so its own result/warning processing can find the artifacts.

For an idempotency check, manually rerun the latest successfully processed batch
with `check_idempotency=true`. From PowerShell inside `airflow/` (replace the date
with that latest batch):

```powershell
docker --% compose exec -T airflow-scheduler airflow dags trigger chaos_ecommerce_daily --conf "{\"batch_date\":\"2026-11-07\",\"check_idempotency\":true}"
```

The capture/verify tasks are no-ops for normal runs. When enabled, they compare
total and selected-date counts in all four RAW tables, total counts in customer
snapshots, customer/product dimensions and the order-item fact, and require
unique, non-null fact keys. The baseline is saved under
`airflow/logs/pipeline/<run_id>/idempotency_counts.json`; comparison counts and
differences appear in `verify_counts` logs. Count changes fail the task and
prevent the DAG success email. The baseline is never silently overwritten;
if capture already wrote a file before failing, inspect it and start a new run
rather than clearing capture. A successful verification can be safely retried.

Use only unchanged input/configuration and an already loaded batch with rows
in every RAW entity. Pause external writers during comparison. This checks
count stability and fact keys, not equality of every field. RAW intentionally
retains source duplicates; this does not require unique RAW event IDs. For new
daily data leave the option off: counts are expected to grow. This check runs
after writes and cannot roll them back if a mismatch is found.

- Scheduled at **10:20 Africa/Lagos**; the batch date is the preceding data
  interval's local calendar date. It does not use the container's wall-clock
  date for scheduled source generation.
- Starts with **2026-11-04**, immediately after the manually loaded and
  snapshotted history through November 3. Catchup is enabled. After 10:20 on
  November 7, batches November 4, 5 and 6 are eligible, processed in order.
- The DAG is paused on creation and allows one active run. Data-processing tasks
  depend on their previous run succeeding; optional count tasks only depend on
  this run's upstream tasks, so introducing them does not block existing runs.
  The new RAW-test task also starts without a prior-run dependency.
  There are two retries with a two-minute delay.
- An optional `batch_date` parameter is available on the manual trigger form.
  Supply it for manual runs. Only completed dates from November 4 onward are
  accepted. Historical RAW reloads before that date remain a separate manual
  workflow; do not use this DAG to reset/replay existing snapshot history.
- Snapshot and build receive the same date. Snapshots read the exact customer
  batch, and their guard rejects source versions older than captured history.
- Simulator history anchor/seed settings are left unchanged for deterministic
  regeneration. A retry overwrites the same source files; RAW reload replaces
  the batch, snapshot timestamp strategy preserves existing versions, and the
  fact uses MERGE. Out-of-order old replays can fail the snapshot guard even
  when RAW reload itself is idempotent.

## Configuration and mounts

`airflow/.env` configures Compose/Airflow (UID, UI login, Fernet key, JWT key).
Root `.env` contains the pipeline's Snowflake settings and is supplied through
the worker's `env_file`. It is not copied into the image or printed by tasks.
The Docker build context excludes credentials, private keys, data and logs.

Compose mounts simulator/ingestion/scripts read-only, shares generated `data/`,
and mounts the dbt project so `dbt deps` can populate its package directory.
The container uses the committed `profiles.yml.example` as its credential-free
profile. This first local DAG uses `--target dev` with password authentication.
Key-pair-only use requires a Linux key mount and a prod-target adaptation; a
Windows private-key path is not usable inside Docker.

Standalone dbt task JSON artifacts and logs go under
`airflow/logs/pipeline/<run-id>/<task-id>/`; Cosmos task output is captured in
the corresponding Airflow task logs. Failures and retries are visible in
the Airflow task logs. The final dbt build runs the source/model tests selected
by staging, intermediate and marts. Freshness runs separately after RAW loads.
Freshness measures ingestion time, not the historical order dates.

## Build and review without running the pipeline

From the `airflow` folder, with Docker Desktop running:

```powershell
docker compose config --quiet
docker compose build airflow-worker
```

The image is shared by all Airflow services. To validate only DAG imports,
dependencies and date templates, without scheduling or running any task:

```powershell
docker compose run --rm --no-deps --entrypoint python airflow-worker /opt/pipeline/scripts/validate_dag.py
```

For the complete non-warehouse check (DAG, worker configuration, imports,
dependency consistency, dbt packages and parsing):

```powershell
docker compose run --rm --no-deps --entrypoint bash airflow-worker /opt/pipeline/scripts/validate_container.sh
```

This uses a fixed completed date as a validation fixture. It does not generate
or load that date, query source freshness, execute snapshots or build models.

Once the image and checks pass, apply the updated Compose configuration:

```powershell
docker compose up -d --no-build
docker compose ps
```

This preserves the existing PostgreSQL named volume and Airflow login/history.
It recreates Airflow service containers to use the custom image and new mounts.
Do not use `down --volumes` to apply these changes.

Open http://localhost:8080 and find **chaos_ecommerce_daily**. Review its graph
and keep it paused until you are ready for writes to your Snowflake project.
After confirming manual snapshot history is complete through November 3,
unpause it to allow chronological catchup. No warehouse access occurs at DAG
import time; actual loads/snapshots/builds happen only when tasks execute.

Do not run `airflow dags test` as an import check: it executes tasks, including
Snowflake writes. Do not trigger a newer manual batch ahead of pending older
scheduled dates. If a task fails, inspect its log, fix the cause, then clear or
retry the failed current run. Do not delete snapshot tables or volumes to retry.

When Docker Desktop or the computer is off, runs stop. On restart, scheduled
catchup resumes the backlog. Daily unattended service requires an always-on host.

## Fresh clone

Create root `.env` and local profiles following the repository README. Copy
`airflow/.env.example` to `airflow/.env`, fill the generated Fernet/JWT keys,
and initialize the metadata database with `docker compose up airflow-init`.
Existing initialized installations do not need their metadata volume reset.
The generated `airflow.cfg`, task logs and local environment files are ignored
by Git. Startup/build and validation do not imply a successful live DAG run.
