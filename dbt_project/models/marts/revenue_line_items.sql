{{ config(materialized='ephemeral') }}

with checked as (
    select
        f.*,
        c.country as customer_country,
        c.region as customer_region,
        case when
            not f.is_quantity_invalid
            and f.quantity > 0
            and f.unit_price >= 0
            and f.discount_amount >= 0
            and f.line_total >= 0
            and f.line_total = round(f.quantity * f.unit_price - f.discount_amount, 2)
        then true else false end as is_line_valid,
        case when
            f.unit_cost >= 0
            and f.cost_amount = round(f.quantity * f.unit_cost, 2)
        then true else false end as is_cost_valid
    from {{ ref('fct_order_items') }} f
    left join {{ ref('dim_customer') }} c on f.customer_key = c.customer_key
)

select
    *,
    case when is_revenue_eligible and is_line_valid and not is_order_missing
        then true else false end as is_reportable
from checked
