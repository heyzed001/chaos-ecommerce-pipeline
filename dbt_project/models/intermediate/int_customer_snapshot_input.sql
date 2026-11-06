{{ config(materialized='ephemeral') }}

-- Inline this date-controlled RAW projection into the snapshot. A view would
-- retain a replay date and could accidentally pin later daily snapshot runs.
{{ customer_snapshot_rows() }}
