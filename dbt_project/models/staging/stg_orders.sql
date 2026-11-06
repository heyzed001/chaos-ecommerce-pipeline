with typed as (
    select
        nullif(trim(_record:order_id::varchar), '') as order_id,
        nullif(trim(_record:customer_id::varchar), '') as customer_id,
        {{ source_timestamp('_record:order_placed_at') }} as order_placed_at,
        lower(trim(_record:order_status::varchar)) as order_status,
        upper(trim(_record:order_currency::varchar)) as order_currency,
        _record:shipping_cost::number(12, 2) as shipping_cost,
        _record:order_total::number(12, 2) as order_total,
        lower(trim(_record:payment_method::varchar)) as payment_method,
        nullif(lower(trim(_record:loyalty_tier::varchar)), '') as loyalty_tier,
        {{ source_timestamp('_record:updated_at') }} as updated_at,
        _event_id,
        _batch_date,
        _source_file,
        _loaded_at
    from {{ source('raw', 'orders') }}
)

select *
from typed
qualify row_number() over (
    partition by order_id
    order by updated_at desc nulls last, _batch_date desc, _event_id desc
) = 1
