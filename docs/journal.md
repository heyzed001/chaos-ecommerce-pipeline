## Phase 2 — Chaos simulator

Measured across 30 days (2026-10-05 to 2026-11-03), seed 42:
2,330 order rows, 5,747 line items, 500 customers snapshotted daily.

| Defect | Configured | Measured |
|---|---|---|
| Same order twice in one file | 3% | 82 orders |
| Order re-emitted next day | 3% | 50 orders |
| Late arrival | 5% | 155 rows (6.7%), lag 1-7 days |
| Negative quantity | 1% | 77 lines |
| Null email / phone | 2% / 8% | 1.0% / 6.4% |
| Header total != sum of lines | 2% | 55 orders (2.5%) |
| Schema drift | off | enabled in Phase 11 |
| Customers who changed region | 15% | 79 of 500 |

Decisions:
- No wall-clock timestamps anywhere. Regenerating a single day produced a
  byte-identical file to the backfill's, verified by checksum.
- Simulator is stateless; it never reads yesterday's output.
- `updated_at` tracks the version's effective date, not the batch date.
  Otherwise dbt snapshot would record a new version daily for every customer.
- `unit_cost` sits on the order line, not the dimension, so margin does not
  restate when a product is repriced.

Fixed during the phase: the first run produced only 5 region changes because
moves were spread over four years. Added `history_anchor` / `history_days` so
moves cluster inside the generated window, plus a test that fails below 5%.


## Phase 4 — Schema drift decision

**Chosen: VARIANT in RAW.** Each source record is stored whole in a single
`_record VARIANT` column, with only load metadata beside it. The COPY statement
reads `$1`, the entire Parquet row, so it never names a business column and the
column count in the file cannot affect it.

**Rejected: typed columns with no evolution.** COPY would fail the day the
source adds a field. A hard contract is defensible, but RAW is the evidence of
what arrived, and a layer that refuses data destroys that evidence.

**Rejected: typed columns with ENABLE_SCHEMA_EVOLUTION = TRUE.** Works, but
Snowflake alters the table on its own. This pipeline exists to make defects
observable, so absorbing a schema change invisibly is the wrong default.

**Cost accepted:** staging must cast every field out of `_record`. That is
Phase 6's job, and it exercises VARIANT handling, which is a Snowflake skill
worth demonstrating.

**Verified:** generated 2026-11-04 with `--schema-drift-date 2026-11-04` and
loaded it with no code or DDL change. Querying `_record:loyalty_tier` returns
NULL for earlier batches and a value from the cutover date onward.

## Phase 7 - Customer history and intermediate models

Implemented `snap_customers` with dbt's timestamp strategy, keyed by
`customer_id` and driven by source `updated_at`. A date-controlled ephemeral
RAW projection lets historical batches be replayed oldest to newest without
changing the current-state staging view. The snapshot pre-hook rejects empty
or invalid input and versions older than current captured history; it never
resets the snapshot table automatically.

`int_customer_history` exposes `valid_from`, `valid_to`, `is_current` and a
customer version ID. `int_order_items_enriched` joins orders, current product
attributes and the customer version effective at order time. Line cost and
margin use the order item's `unit_cost`, not current catalogue cost. Cancelled
and returned orders stay visible with zero status-eligible revenue. Invalid
quantities, missing costs and unmatched joins are flagged. Shipping is not
allocated to order lines.

Added checks for one current customer version, consecutive valid intervals,
coverage of loaded RAW customer versions, and preservation of item counts.
Local validation: dbt parsing passed; eight offline tests passed covering the
snapshot guard, dated input and rendered Snowflake SQL syntax. Local Parquet
history contains 79 customers with multiple versions; CUST-00001 changes from
Lagos to Ashanti effective 2026-10-31 02:00:00.

Warehouse execution is pending the user's manual replay and tests. No snapshot
was executed or Snowflake views created as part of this implementation.
Runbook: `dbt_project/models/intermediate/README.md`.

## Phase 8 - Date dimension

Added `dim_date` as a MARTS table using Snowflake's generated calendar and
gap-free ROW_NUMBER offsets. Inclusive bounds are configurable using
`dim_date_start_date` and `dim_date_end_date`; defaults cover 2024-01-01 through
2028-12-31 (1,827 days). Calendar generation does not depend on orders, the
current date, snapshots or a database query during compilation.

The stable date key is YYYYMMDD. Attributes include calendar day/month/quarter/
year, English names, ISO weekday/week/year, weekends, period boundaries and
leap-year status. No national holiday or fiscal calendar rules are assumed.

Added warehouse tests for key uniqueness/nulls, allowed values, expected
inclusive range, day continuity, key consistency and February lengths. Local
dbt parsing passed; eight new calendar tests and eight existing snapshot/SQL
checks passed offline. Snowflake build and warehouse tests remain for manual
execution: `dbt build --project-dir dbt_project --profiles-dir dbt_project --select dim_date`.

## Phase 8 - Product/customer dimensions and incremental fact

Added Type 1 `dim_product` with a stable hash of product_id, and SCD2
`dim_customer` with a stable hash of customer_id plus explicitly formatted
valid_from. Customer versions come from the existing snapshot history.

`fct_order_items` is incremental with Snowflake MERGE on order_item_id. The
arrival-batch lookback is now eight days, with independent inclusion of unseen
IDs, line/header load-timestamp corrections and changed dimension keys/revenue
eligibility. Old backfills are not excluded solely by business date. Target
MERGE matching is not restricted to recent dates. Charged costs are retained;
source defects remain flagged and status-based revenue is not certified revenue.

Added dimension/fact key and relationship tests, a customer-version time check,
and source/fact completeness and value reconciliation. Local validation:
dbt parsing passed and 26 offline checks passed. Tests execute fact candidate
selection SQL against in-memory fixtures for old backfills/corrections,
dimension-key repairs, revenue changes, empty targets and invalid lookbacks.
Live Snowflake MERGE and warehouse data tests remain pending manual execution.

Runbook: `dbt_project/models/marts/README.md`. Complete chronological snapshot
replay first, then build staging, intermediate and marts without selecting the
snapshot again. Source deletes or untracked rule/schema changes require an
intentional fact full refresh; snapshot history is never reset automatically.

## Phase 8 - Point-in-time fact join and revenue summaries

Confirmed that the order timestamp selects customer history using the interval
`valid_from <= order_placed_at < valid_to`, with NULL valid_to for current rows.
Added an offline test of the actual join expression: an order just before a move
uses the old version, while orders exactly at/after the move use the new version.

Added `mart_daily_revenue` and `mart_daily_region_revenue`, with shared ephemeral
reporting rules. Both group by order date and currency; regional geography uses
the historical customer key. Invalid lines and excluded statuses do not contribute
to revenue, but diagnostic counts retain visibility. Missing costs preserve sales
and make group cost/margin NULL. Unknown geography stays in a NULL regional group.
Shipping and reported header totals are excluded from line revenue.

Added summary-grain, date, reconciliation and arithmetic checks. All 33 offline
tests passed, including address-change boundaries, revenue exclusions, discounts,
missing costs, unknown regions and currency separation. These local checks do not
execute Snowflake builds or establish that warehouse tests have passed.

## Review fixes - 2026-11-06

Fixed NULL-count handling in the customer snapshot guard and the test requiring
exactly one current version. Added regressions for clean input returning NULL
and customers whose versions are all closed.

Standardized configuration on a root `.env` with a credential-free `.env.example`.
Ingestion keeps the old `.gitignore/.env` only as a fallback. The PowerShell
helper resolves root `.env` independently of the working directory and prints
no values. Existing local configuration was copied into ignored root `.env`.

Added `dbt_project/profiles.yml.example` and fresh-clone instructions. Actual
profiles remain local and ignored. Replaced the inactive `dbt_project/gitignore`
with a working folder-local `.gitignore`, including `.user.yml` and generated
artifacts. Confirmed both templates can be committed while real secrets cannot.
Declared sqlglot explicitly for offline tests and raised dbt-core's minimum to
1.10.5 to match the test argument syntax used in this project.

The idempotency checker now rejects missing/partial local batches and incomplete
reload results rather than reporting PASS after no reload. It remains a row-count
check, not proof of identical contents. All 80 offline tests passed; dbt parsing
with the new profile template and PowerShell root-env loading passed. No
Snowflake models, snapshot replays, commits or pushes were executed for these fixes.

## Generic model tests - 2026-11-07

Expanded generic tests across all 14 dbt models and the customer snapshot.
Coverage now includes unique/non-null business and version keys, required
fields, real foreign-key relationships, categorical domains, booleans and
calendar attributes. Summary grains use composite uniqueness tests. SCD2
customer IDs repeat legitimately and are not tested as unique natural keys.

Kept intended nullable fields nullable: contacts, optional loyalty tier,
valid_to for current versions, missing unit costs and unknown summary geography.
Non-null quantity tests do not replace future positive-quantity business checks.
The current source currency contract is NGN; new currencies require an explicit
accepted-values update rather than being converted or combined automatically.

Validation: dbt parsing passed with 310 generic and 10 singular tests. All 80
offline Python checks passed. New warehouse tests remain for manual execution;
the earlier successful build does not certify these newly added definitions.
Policy and command: `dbt_project/tests/README.md`.

## Phase 10 - Airflow orchestration - 2026-11-07

Added the paused `chaos_ecommerce_daily` DAG with configuration validation,
dbt package preparation, deterministic daily generation, RAW loading, source
freshness, dated customer snapshots and tested dbt model builds. Schedule is
03:00 Africa/Lagos; the first catchup batch is November 4, after manual history
through November 3. Runs are serial with dependencies on prior-run success,
two retries and explicit task/run timeouts. Manual batch dates are validated.

Extended Compose with pipeline mounts and a custom Airflow 3.2.2/Python 3.12
image. dbt and ingestion dependencies are isolated in `/opt/pipeline-venv`.
Snowflake settings are supplied only to the worker from ignored root `.env`;
credentials are not included in the image. Added artifact/config exclusions,
Linux shell line-ending rules and a manual setup/recovery runbook.

Validation: image built; dependency consistency and imports passed; dbt deps
and parsing passed inside a one-off worker. All 90 local tests and six real
Airflow import/template/scheduling checks passed. No DAG tasks, Snowflake
queries, loads, snapshots or dbt builds were executed by these validation steps.
The existing running services were not recreated. Apply the new image/mounts
with `docker compose up -d --no-build` in airflow/, then review/unpause the DAG.
Existing PostgreSQL volumes and login/history are preserved; do not reset them.

Runbook: `airflow/README.md`. Live DAG execution remains pending the user's
activation. No changes from this phase were committed or pushed.

## Failure notifications - Gmail and Slack

Added a task-level final-failure callback that attempts Gmail/SMTP and Slack
independently. Each channel is disabled until configured in ignored
`airflow/.env`. Email uses TLS/SSL and a local App Password; Slack uses an HTTPS
incoming webhook. No secrets are embedded in source or sent in alert bodies.
Notifications include the failed task, DAG, run, batch date, error class and a
log/dashboard link; raw exception text and environment variables are excluded.

Added disabled local settings and documented Gmail App Password and Slack
setup in `airflow/ALERTS.md`. The user still needs to provide sender/recipient,
App Password and webhook locally before enabling channels and recreating
services. There is no external monitor for a stopped laptop/Airflow instance.

Validation: 100 local tests passed with all SMTP/HTTP delivery mocked; six real
Airflow import/template checks passed, including callback attachment. No test
messages were sent, and no DAG or Snowflake tasks ran. Live notification
delivery is not yet verified. These changes have not been committed or pushed.

Updated the daily schedule to 10:20 Africa/Lagos (10:20 AM) at the user's request,
including the initial interval alignment. Set the local email recipient to
kayodeabdulaziz@gmail.com. Gmail sender, SMTP username and App Password are
still missing; email remains disabled. No test email or Slack message was sent.

At the user's request, unpaused the DAG for immediate execution on November 7.
An existing November 4 run had been paused after configuration validation.
It resumed and all seven tasks succeeded, including live RAW loading, freshness,
the dated snapshot and dbt build/tests. Catchup continues serially under the
10:20 Lagos schedule. Changing the timetable also produced a second scheduled
interval for the same November 4 batch; this reprocesses the date through the
existing idempotent load/snapshot/MERGE behavior rather than appending copies.
Gmail and Slack remain disabled because sender credentials and webhook are absent.

## Brevo notifications and rerun count verification - 2026-11-08

Current status supersedes the earlier pending Gmail setup: email is now enabled
through Brevo SMTP, using port 587 and STARTTLS. The test message was accepted
by Brevo and the user confirmed receipt at kayodeabdulaziz@gmail.com. Initial
SMTP authentication attempts returned 525 due to IP restrictions; the user
disabled SMTP IP blocking because the outgoing IP changes. Credentials remain
in ignored airflow/.env. Running Airflow services were recreated with the
working settings and verified healthy; no pipeline runs were active then.

Task failures send an email after the initial attempt plus two retries. Added
a DAG-level success callback to send one completion email per successful run,
with batch date, run ID, start/end times and dashboard link. It applies to new
completions, not earlier runs. Failures in notification delivery are logged
without raw secret-bearing exception messages and do not undo pipeline work.
Automatic success delivery still needs confirmation from a real DAG completion;
the successful SMTP test alone does not prove callback execution.

Slack/webhook configuration is deferred at the user's request and stays disabled.
The daily timetable remains 10:20 Africa/Lagos. Local Docker/Airflow must be
running and online for execution and notifications; no external uptime monitor
or durable notification delivery queue has been added.

Added ingestion/check_idempotency.py and optional capture_counts/verify_counts
tasks around generation, ingestion, snapshots and dbt builds. Enable with the
boolean manual-run parameter check_idempotency=true for an unchanged, already
loaded latest batch. Normal daily runs leave it false and perform no count queries.
Capture records all four RAW totals and selected-date counts, customer snapshot
and customer/product dimension totals, fact total, and duplicate/null fact key
counts. Verify fails on any count change or invalid fact keys. Baselines are
stored per run in ignored Airflow logs and never silently overwritten. This is
a read-only check around pipeline writes, not a rollback mechanism or a proof
that every row's contents are identical. RAW source duplicates remain allowed.
Run without concurrent external writers. See airflow/README.md for the command.

Validation: 50 offline Airflow/ingestion checks passed with transport and database
calls mocked. All eight actual Airflow DAG import/render checks passed in Docker.
No live warehouse count comparison or pipeline rerun was launched for this change;
those remain manual so a passing offline suite is not reported as live idempotency.
Notification, schedule, count-check and documentation changes are being committed
and pushed together at the user's request; local secrets and generated logs are excluded.
