# Customer history and intermediate models

## What is implemented

- `int_customer_snapshot_input` is ephemeral: it is inlined into the snapshot
  query and never creates a dated view. With `customer_snapshot_date` supplied,
  it selects exactly that RAW customer batch. Without the variable, it selects
  each customer's latest known source version across RAW.
- `snap_customers` is a dbt timestamp snapshot, keyed by `customer_id` and
  driven by `updated_at`. Source effective times define the SCD2 intervals.
  Reload timestamps and daily file metadata are excluded from its payload.
- `int_customer_history` exposes `customer_version_id`, `valid_from`,
  `valid_to` and `is_current`. Intervals include the start and exclude the end.
- `int_order_items_enriched` keeps one row per item, joins order details and
  current product attributes, and joins customer history using order time.
  It calculates gross amount, charged line cost, margin and status-eligible
  revenue. Cancelled and returned orders are retained with zero eligible
  revenue; shipping is not allocated to lines. Invalid quantities, missing
  costs and unmatched joins are flagged, not silently repaired or filtered.
  A missing historical match never falls back to today's customer region.

The snapshot uses `SNAPSHOTS` for dev/prod; other targets follow the existing
schema-naming macro (for example `CI_SNAPSHOTS`). The Snowflake role needs
permission to create/use that schema and manage the snapshot table. As with
the rest of this project, dev/prod should use separate databases for isolation.

## First historical replay (manual)

First load every required RAW customer batch. If only one day is loaded,
load the full range yourself using the ingestion CLI. Do not snapshot the
latest state first and then attempt to replay earlier days: snapshots cannot
insert old versions into already-recorded history. The pre-hook rejects input
with missing keys/timestamps, no rows, or timestamps older than current history.
It does not drop, truncate or reset any existing snapshot table.

From the project root, in the activated virtual environment, load your
environment variables as before:

```powershell
. .\dbt_project\load_env.ps1
```

Then snapshot the loaded dates in ascending order. This loop writes the
customer snapshot in Snowflake; it does not reload RAW data:

```powershell
$firstDay = [datetime]'2026-10-05'
$lastDay = [datetime]'2026-11-03'
for ($batchDay = $firstDay; $batchDay -le $lastDay; $batchDay = $batchDay.AddDays(1)) {
    $snapshotVars = "customer_snapshot_date: '" + $batchDay.ToString('yyyy-MM-dd') + "'"
    dbt snapshot --project-dir dbt_project --profiles-dir dbt_project --select snap_customers --vars $snapshotVars
    if ($LASTEXITCODE -ne 0) { throw 'Snapshot replay stopped; inspect the failed date before continuing.' }
}
```

If a replay was interrupted, continue from the failed date, not the beginning.
Re-running the same current input should not add a version. If the guard rejects
an older input, inspect existing history before choosing a separate target or a
deliberate reset; this implementation never resets it for you.

Build the intermediate views and run the history checks afterward:

```powershell
dbt build --project-dir dbt_project --profiles-dir dbt_project --select intermediate
dbt test --project-dir dbt_project --profiles-dir dbt_project --select snap_customers customer_history_one_current customer_history_valid_intervals customer_history_covers_loaded_versions enriched_preserves_item_count
```

`customer_history_covers_loaded_versions` verifies that every distinct loaded
RAW customer address/version appears in history. It fails if a backfill was
reduced to the latest version before being snapshotted. Interval tests reject
overlaps/gaps between captured versions and multiple open versions. The item
count check detects joins that duplicate or drop order items.

To inspect an actual address change, run this read-only SQL in Snowsight:

```sql
select customer_id, address_line, region, valid_from, valid_to, is_current
from ecommerce.staging.int_customer_history
qualify count(*) over (partition by customer_id) > 1
order by customer_id, valid_from;
```

The more specific query in `analyses/verify_customer_address_changes.sql`
compares consecutive addresses and regions. A customer move should create a
second row, close the first interval at the new version's effective time, and
leave exactly one current row. The first observed version starts at its source
`updated_at`; the pipeline cannot recover versions absent from RAW.

## Daily operation after historical replay

After loading a new daily RAW batch:

```powershell
dbt snapshot --project-dir dbt_project --profiles-dir dbt_project --select snap_customers
dbt build --project-dir dbt_project --profiles-dir dbt_project --select staging intermediate
```

No replay date is supplied during normal daily operation. Timestamp strategy
assumes the source advances `updated_at` whenever tracked details change.
Hard deletes are not inferred from missing rows. A correction with an unchanged
timestamp is not captured automatically. Detecting and repairing such cases is
a separate source-contract decision.

These commands have been provided for manual execution. Local parsing and SQL
syntax checks do not prove the warehouse replay, permissions or data tests pass.
