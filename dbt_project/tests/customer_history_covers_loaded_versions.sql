-- Detect missed versions, including the mistake of snapshotting only the
-- latest customer state once after loading an entire backfill.
-- During a dated replay, only validate batches up to the replay date.
{% set batch_date = customer_snapshot_date() %}
with expected as (
    select distinct
        nullif(trim(_record:customer_id::varchar), '') as customer_id,
        {{ source_timestamp('_record:updated_at') }} as updated_at,
        nullif(trim(_record:address_line::varchar), '') as address_line,
        nullif(trim(_record:region::varchar), '') as region
    from {{ source('raw', 'customers') }}
    {% if batch_date is not none %}
    where _batch_date <= '{{ batch_date }}'::date
    {% endif %}
)

select e.*
from expected e
left join {{ ref('int_customer_history') }} h
    on e.customer_id = h.customer_id
    and e.updated_at = h.valid_from
    and e.address_line is not distinct from h.address_line
    and e.region is not distinct from h.region
where h.customer_version_id is null
