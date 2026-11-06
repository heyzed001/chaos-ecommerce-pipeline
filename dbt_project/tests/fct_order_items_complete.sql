-- Detect missed late arrivals, stale/deleted rows, and incorrect updates.
-- The incremental model does not automatically delete disappeared source IDs.
select
    coalesce(e.order_item_id, f.order_item_id) as order_item_id
from {{ ref('int_order_items_enriched') }} e
full outer join {{ ref('fct_order_items') }} f on e.order_item_id = f.order_item_id
where e.order_item_id is null
    or f.order_item_id is null
    or e.quantity is distinct from f.quantity
    or e.unit_price is distinct from f.unit_price
    or e.unit_cost is distinct from f.unit_cost
    or e.discount_amount is distinct from f.discount_amount
    or e.line_total is distinct from f.line_total
    or e.line_cost is distinct from f.cost_amount
    or e.line_margin is distinct from f.margin_amount
    or e.revenue_amount is distinct from f.revenue_amount
    or e.order_status is distinct from f.order_status
