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
