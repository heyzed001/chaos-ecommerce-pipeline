with typed as (
    select
        nullif(trim(_record:product_id::varchar), '') as product_id,
        nullif(trim(_record:product_name::varchar), '') as product_name,
        nullif(trim(_record:category::varchar), '') as category,
        nullif(trim(_record:subcategory::varchar), '') as subcategory,
        nullif(trim(_record:brand::varchar), '') as brand,
        _record:list_price::number(12, 2) as list_price,
        _record:cost_price::number(12, 2) as cost_price,
        _record:is_active::boolean as is_active,
        {{ source_timestamp('_record:created_at') }} as created_at,
        {{ source_timestamp('_record:updated_at') }} as updated_at,
        _event_id,
        _batch_date,
        _source_file,
        _loaded_at
    from {{ source('raw', 'products') }}
)

select *
from typed
qualify row_number() over (
    partition by product_id
    order by updated_at desc nulls last, _batch_date desc, _event_id desc
) = 1
