# Data-test policy

Generic tests are declared beside the resources in `models/*/_*.yml` and
`snapshots/snap_customers.yml`. All 14 models and the customer snapshot have
coverage, including the two ephemeral inputs, whose tests inline their SQL.

- `unique` and `not_null` protect one-row business keys and surrogate/version
  keys. SCD2 `customer_id` is intentionally not unique across history.
- `relationships` checks foreign keys against the appropriate staging model,
  dimension, or snapshot version. It is used where there is a real referenced
  entity; the standalone calendar does not have an invented foreign key.
- `accepted_values` checks order statuses, payment methods, customer segments,
  catalogue categories, optional loyalty tiers, the current NGN currency
  contract, booleans and calendar domains. Update currency lists deliberately
  if new source currencies are introduced; summaries still separate currencies.
- `dbt_utils.unique_combination_of_columns` checks the date/currency grain of
  summaries, the regional grain including geography, and customer-version grain.

Accepted-values checks are paired with not-null checks for required categories:
an accepted-values test alone does not reject NULLs. Email/phone, optional
loyalty tier, open-ended valid_to, deliberately missing historical costs, and
unknown regional geography remain nullable. Invalid quantities are retained as
flags: non-null checks do not certify a quantity as positive. Cost/margin totals
may be NULL when eligible costs are incomplete. Those are documented data rules,
not reasons to apply not_null indiscriminately.

Run against previously built models and populated customer snapshots:

```powershell
. .\dbt_project\load_env.ps1
dbt test --project-dir dbt_project --profiles-dir dbt_project
```

This runs data tests without rebuilding models or replaying snapshots. The
current project parses 310 generic tests plus 10 singular tests. Source freshness
is separate (`dbt source freshness`). Offline Python checks do not establish
that these warehouse tests passed. No Snowflake tests were executed when these
definitions were added.
