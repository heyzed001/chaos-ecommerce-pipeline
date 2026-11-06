# Staging models

These models become views in STAGING. They extract typed fields from RAW's
`_record`, normalize text, retain load metadata and deduplicate business keys.
Ordering uses source version timestamps, batch dates and event IDs rather than
reload timestamps, so reloading an identical batch does not change the winner.

Customers and products expose their latest known state. Their complete daily
history remains in RAW. Taking one dbt snapshot after a full backfill cannot
reconstruct all previous customer moves. Use the date-controlled snapshot replay
described in `../intermediate/README.md` to capture loaded RAW history.

Missing customer contacts are kept and flagged, rather than dropping customers
and breaking order relationships. Negative quantities and reported header totals
are not corrected or filtered. Business quality tests and reconciliation still
need implementing before marts are built. Strict numeric/boolean casts fail on
incompatible values; unsuccessful date/timestamp conversions become NULL and are
checked by not-null tests on required fields.

`loyalty_tier` is explicitly exposed when present. Other new source fields remain
in RAW until deliberately added to a staging model. Older files without
`unit_cost` produce NULL cost: no current catalogue price is substituted.

The source-type macros handle Parquet epoch-day dates, epoch-integer timestamp
strings and formatted date/timestamp values. This still needs a warehouse run
against the actual file-format configuration.

From the repository root, with the Python virtual environment activated:

```powershell
. .\dbt_project\load_env.ps1
dbt deps --project-dir dbt_project --profiles-dir dbt_project
dbt build --project-dir dbt_project --profiles-dir dbt_project --select staging
```

`dbt build` creates the views and runs selected tests. Customer snapshots and
intermediate models now have a separate manual runbook in
`../intermediate/README.md`. Mart models are not implemented yet.
Source freshness is a separate check:

```powershell
dbt source freshness --project-dir dbt_project --profiles-dir dbt_project
```
