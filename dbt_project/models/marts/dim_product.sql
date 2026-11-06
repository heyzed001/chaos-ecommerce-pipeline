select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    list_price,
    cost_price,
    is_active,
    created_at,
    updated_at
from {{ ref('stg_products') }}
