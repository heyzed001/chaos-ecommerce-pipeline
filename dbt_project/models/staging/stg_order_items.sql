with typed as (
    select
        nullif(trim(_record:order_item_id::varchar), '') as order_item_id,
        nullif(trim(_record:order_id::varchar), '') as order_id,
        nullif(trim(_record:product_id::varchar), '') as product_id,
        _record:line_number::integer as line_number,
        _record:quantity::integer as quantity,
        _record:unit_price::number(12, 2) as unit_price,
        _record:unit_cost::number(12, 2) as unit_cost,
        _record:discount_amount::number(12, 2) as discount_amount,
        _record:line_total::number(12, 2) as line_total,
        _event_id,
        _batch_date,
        _source_file,
        _loaded_at
    from {{ source('raw', 'order_items') }}
)

select *
from typed
qualify row_number() over (
    partition by order_item_id
    order by _batch_date desc, _event_id desc
) = 1
