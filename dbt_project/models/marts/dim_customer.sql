select
    {{ dbt_utils.generate_surrogate_key([
        'customer_id', "to_char(valid_from, 'YYYY-MM-DD HH24:MI:SS.FF9')"
    ]) }} as customer_key,
    customer_version_id,
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
    valid_from,
    valid_to,
    is_current
from {{ ref('int_customer_history') }}
