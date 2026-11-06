-- After replay, this returns customers with actual captured address changes.
with versions as (
    select
        *,
        row_number() over (partition by customer_id order by valid_from) as version_number,
        lag(address_line) over (partition by customer_id order by valid_from) as previous_address,
        lag(region) over (partition by customer_id order by valid_from) as previous_region
    from {{ ref('int_customer_history') }}
)

select
    customer_id,
    previous_address,
    address_line,
    previous_region,
    region,
    valid_from,
    valid_to,
    is_current
from versions
where version_number > 1
    and (address_line is distinct from previous_address or region is distinct from previous_region)
order by customer_id, valid_from
