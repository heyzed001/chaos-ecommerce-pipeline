select f.order_item_id, f.customer_key, f.order_placed_at
from {{ ref('fct_order_items') }} f
left join {{ ref('dim_customer') }} c on f.customer_key = c.customer_key
where c.customer_key is null
    or f.customer_id is distinct from c.customer_id
    or f.order_placed_at < c.valid_from
    or (c.valid_to is not null and f.order_placed_at >= c.valid_to)
