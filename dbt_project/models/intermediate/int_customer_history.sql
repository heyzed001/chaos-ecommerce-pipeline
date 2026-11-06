select
    dbt_scd_id as customer_version_id,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address_line,
    city,
    region,
    country,
    postal_code,
    signup_date,
    customer_segment,
    updated_at,
    dbt_valid_from::timestamp_ntz as valid_from,
    dbt_valid_to::timestamp_ntz as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_customers') }}
