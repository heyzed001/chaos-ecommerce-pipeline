# Marts

## Product and customer dimensions

`dim_product` rebuilds the latest catalogue as a Type 1 dimension. Its stable
`product_key` is a dbt_utils hash of `product_id`. Prices in this dimension are
current values and never replace costs charged on historical order lines.

`dim_customer` rebuilds all captured SCD2 versions from `int_customer_history`.
Its stable `customer_key` hashes `customer_id` and `valid_from` formatted with
explicit nanosecond precision. It excludes mutable addresses and reload times.
Both closed and current versions are retained, with `valid_from`, `valid_to`
and `is_current`. The existing snapshot must be populated through chronological
replay first; see `../intermediate/README.md`.

## Incremental order-item fact

`fct_order_items` uses dbt's Snowflake `merge` strategy with the non-null natural
key `order_item_id`. The first run creates the table; later runs insert unseen
IDs and update matching rows. There is no restricted target-side date predicate,
so MERGE can match older rows without inserting duplicates. Staging and join
tests guard the requirement of one source row per key.

Incremental candidates include:

- Items within the arrival-batch lookback (eight days by default).
- Any ID absent from the existing fact, even an older backfill.
- Rows whose line or header RAW load timestamp reaches the last processed load
  watermark, including corrections to old orders. Equality permits timestamp ties.
- Rows whose resolved customer/product/date key or revenue eligibility changed.

This is based on loaded batches and ingestion timestamps, not wall-clock time or
only the order's business date. The enriched view exposes the most recent of its
line/header load timestamps. The query still scans source views and compares
keys across the fact: correctness is prioritized over scan minimization for this
portfolio dataset. It does not claim to be a high-volume CDC implementation.

Fact customer keys resolve the captured version valid at order time via the
enriched input. Product keys refer to current Type 1 products. Date keys join
the calendar. Missing dimension matches are preserved as NULL keys and fail
relationship/not-null checks instead of silently dropping sales.

Measures include quantity, charged unit price/cost, discount, line total,
`cost_amount`, `margin_amount` and status-based `revenue_amount`. Shipping and
header totals are not repeated on each line. Intentional source defects remain
flagged. Revenue eligibility excludes cancelled/returned statuses; it is not
a guarantee of valid quantities or reconciled header totals. Business-quality
rules still need completion before using these amounts as certified metrics.

After snapshot replay and with environment variables loaded, build manually:

```powershell
dbt build --project-dir dbt_project --profiles-dir dbt_project --select staging intermediate marts
```

This deliberately excludes snapshots: running the latest snapshot first would
prevent an initial chronological history replay. Refresh intermediate views to
expose `_source_loaded_at` before running the new fact for the first time.

To exercise a second incremental MERGE without reloading RAW:

```powershell
dbt build --project-dir dbt_project --profiles-dir dbt_project --select fct_order_items
```

Compare the following read-only query before and after. With unchanged input,
counts and business-value hash should remain unchanged:

```sql
select count(*) as rows_loaded,
       count(distinct order_item_id) as unique_items,
       hash_agg(order_item_id, customer_key, product_key, order_date_key,
                quantity, line_total, unit_cost, order_status) as business_hash
from ecommerce.marts.fct_order_items;
```

Hard deletes are not propagated by this incremental model. Full source-vs-fact
reconciliation detects stale or missing rows. If source IDs disappear, business
logic changes beyond the tracked fields, or the fact's schema changes, deliberately
rebuild only the fact:

```powershell
dbt build --full-refresh --project-dir dbt_project --profiles-dir dbt_project --select fct_order_items
```

This does not reset snapshot history. Do not change surrogate-key definitions
without rebuilding dependent facts. Offline tests execute the actual candidate
selection SQL against fixtures; live Snowflake MERGE execution remains a manual
validation step.

## Revenue summaries

Two rebuilt tables summarize the current fact, so late-arrival updates appear
after the fact MERGE and the next summary build:

- `mart_daily_revenue`: one row per order date and currency.
- `mart_daily_region_revenue`: one row per order date, currency, historical
  customer country and region. Geography comes from the fact's customer-version
  key, never a join to the customer's current row.

The shared ephemeral `revenue_line_items` model applies the reporting policy.
Revenue includes only status-eligible, matched orders with positive quantities,
non-negative prices/discounts/totals, and consistent line arithmetic. Invalid
lines and cancelled/returned orders remain in observed/diagnostic counts but
contribute zero revenue, units and costs. Diagnostic counts can overlap.
Revenue is based on order date and line totals after discounts, not batch date,
shipping or reported header totals. This is an order-line sales measure, not a
payment settlement or formal revenue-recognition calculation.

Missing/invalid unit costs do not remove an otherwise valid sale. Instead, the
affected summary group's cost and margin are NULL if any included line has an
unusable cost; a count identifies the incomplete lines. Regional NULL geography
groups retain unmatched customers rather than losing their sales in a join.
Currencies are separate and never converted or summed together. The summaries
contain observed date/currency groups, not a zero-filled calendar.

Fields include distinct eligible order count, eligible items, units, gross
sales, discounts, net line revenue, cost, margin, and diagnostic counts for
excluded statuses, invalid lines, missing customer history and incomplete costs.
Group-grain uniqueness, date relationships, regional/daily reconciliation and
accounting arithmetic have warehouse tests. Counts of distinct orders are not
generally additive across arbitrary slices.

After snapshots, intermediate models and facts are current, build manually:

```powershell
dbt build --project-dir dbt_project --profiles-dir dbt_project --select staging intermediate marts
```

For a summary-only rebuild against the existing fact:

```powershell
dbt build --project-dir dbt_project --profiles-dir dbt_project --select mart_daily_revenue mart_daily_region_revenue
```

Refreshing only summaries does not MERGE new RAW arrivals into the fact first.
Do not bypass failing fact-key or history tests when publishing summaries.

## Revenue summaries

Two rebuilt tables summarize the current fact, so late-arrival updates appear
after the fact MERGE and the next summary build:

- `mart_daily_revenue`: one row per order date and currency.
- `mart_daily_region_revenue`: one row per order date, currency, historical
  customer country and region. Geography comes from the fact's customer-version
  key, never a join to the customer's current row.

The shared ephemeral `revenue_line_items` model applies the reporting policy.
Revenue includes only status-eligible, matched orders with positive quantities,
non-negative prices/discounts/totals, and consistent line arithmetic. Invalid
lines and cancelled/returned orders remain in observed/diagnostic counts but
contribute zero revenue, units and costs. Diagnostic counts can overlap.
Revenue is based on order date and line totals after discounts, not batch date,
shipping or reported header totals. This is an order-line sales measure, not a
payment settlement or formal revenue-recognition calculation.

Missing/invalid unit costs do not remove an otherwise valid sale. Instead, the
affected summary group's cost and margin are NULL if any included line has an
unusable cost; a count identifies the incomplete lines. Regional NULL geography
groups retain unmatched customers rather than losing their sales in a join.
Currencies are separate and never converted or summed together. The summaries
contain observed date/currency groups, not a zero-filled calendar.

Fields include distinct eligible order count, eligible items, units, gross
sales, discounts, net line revenue, cost, margin, and diagnostic counts for
excluded statuses, invalid lines, missing customer history and incomplete costs.
Group-grain uniqueness, date relationships, regional/daily reconciliation and
accounting arithmetic have warehouse tests. Counts of distinct orders are not
generally additive across arbitrary slices.

After snapshots, intermediate models and facts are current, build manually:

```powershell
dbt build --project-dir dbt_project --profiles-dir dbt_project --select staging intermediate marts
```

For a summary-only rebuild against the existing fact:

```powershell
dbt build --project-dir dbt_project --profiles-dir dbt_project --select mart_daily_revenue mart_daily_region_revenue
```

Refreshing only summaries does not MERGE new RAW arrivals into the fact first.
Do not bypass failing fact-key or history tests when publishing summaries.

## Date dimension

`dim_date` is a table in `ECOMMERCE.MARTS` for the current dev profile. It has
one row per day, including dates with no orders. Its deterministic generated
calendar requires no RAW data, snapshots or upstream model builds.

Default bounds are **2024-01-01 through 2028-12-31, inclusive**: 1,827 rows,
including February 29 in both 2024 and 2028. Change `dim_date_start_date` and
`dim_date_end_date` in `dbt_project.yml` when a larger calendar is needed.
Reversed ranges are rejected. No current-date-based bounds are used, so a
rebuild retains the same keys and dates.

The Snowflake generator gets a constant rowcount calculated by Jinja.
`ROW_NUMBER()` supplies contiguous date offsets rather than depending on
`SEQ4()` being gap-free. This avoids a database query during compilation,
which the installed `dbt_utils.date_spine` interval-count helper would require.

Columns include the diagram's `date_key` (integer YYYYMMDD), `full_date`,
`day_of_month`, `month`, `month_name`, `quarter`, `year` and `day_of_week`.
Additional fields expose ISO weekday/week/year, weekend status, calendar
period boundaries and leap-year status. ISO weeks start on Monday and their
year can differ from the calendar year near New Year. Names are English.
There are no assumed national holidays, fiscal years or country-specific
working-day rules.

After activating your environment and loading variables, build it manually:

```powershell
dbt build --project-dir dbt_project --profiles-dir dbt_project --select dim_date
```

This creates/replaces the calendar table and runs its selected tests. The tests
check unique/non-null keys and dates, allowed calendar values, inclusive bounds,
expected rowcount, daily continuity, date-key consistency and February lengths.
The Snowflake role needs USAGE and CREATE TABLE access in MARTS.

For future fact-table joins, use `order_date_key = dim_date.date_key` or
`order_date = dim_date.full_date`. Extend the calendar if order dates fall
outside its configured range; do not silently discard those orders.

Local parsing and rendered-SQL checks do not execute this model in Snowflake.
