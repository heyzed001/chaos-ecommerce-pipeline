with typed as (
    select
        nullif(trim(_record:customer_id::varchar), '') as customer_id,
        nullif(trim(_record:first_name::varchar), '') as first_name,
        nullif(trim(_record:last_name::varchar), '') as last_name,
        nullif(lower(trim(_record:email::varchar)), '') as email,
        nullif(trim(_record:phone::varchar), '') as phone,
        nullif(trim(_record:address_line::varchar), '') as address_line,
        nullif(trim(_record:city::varchar), '') as city,
        nullif(trim(_record:region::varchar), '') as region,
        nullif(trim(_record:country::varchar), '') as country,
        nullif(trim(_record:postal_code::varchar), '') as postal_code,
        {{ source_date('_record:signup_date') }} as signup_date,
        lower(trim(_record:customer_segment::varchar)) as customer_segment,
        {{ source_timestamp('_record:updated_at') }} as updated_at,
        _event_id,
        _batch_date,
        _source_file,
        _loaded_at
    from {{ source('raw', 'customers') }}
)

select
    *,
    email is null as is_email_missing,
    phone is null as is_phone_missing
from typed
qualify row_number() over (
    partition by customer_id
    order by updated_at desc nulls last, _batch_date desc, _event_id desc
) = 1
