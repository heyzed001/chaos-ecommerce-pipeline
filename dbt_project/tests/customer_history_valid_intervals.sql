with ordered as (
    select
        *,
        lead(valid_from) over (
            partition by customer_id order by valid_from, customer_version_id
        ) as next_valid_from
    from {{ ref('int_customer_history') }}
)

select *
from ordered
where (valid_to is not null and valid_to <= valid_from)
    or (next_valid_from is not null and (valid_to is null or valid_to != next_valid_from))
    or (is_current != (valid_to is null))
