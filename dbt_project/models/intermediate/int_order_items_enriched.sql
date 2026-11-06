{% set excluded_statuses = var('excluded_revenue_statuses', ['cancelled', 'returned']) %}

with joined as (
    select
        i.order_item_id,
        i.order_id,
        i.product_id,
        i.line_number,
        i.quantity,
        i.unit_price,
        i.unit_cost,
        i.discount_amount,
        i.line_total,
        o.customer_id,
        o.order_placed_at,
        o.order_placed_at::date as order_date,
        o.order_status,
        o.order_currency,
        o.payment_method,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        h.customer_version_id,
        h.region as customer_region_at_order,
        h.country as customer_country_at_order,
        h.customer_segment as customer_segment_at_order,
        o.order_id is null as is_order_missing,
        p.product_id is null as is_product_missing,
        h.customer_version_id is null as is_customer_history_missing,
        i.quantity is null or i.quantity <= 0 as is_quantity_invalid,
        i.unit_cost is null as is_unit_cost_missing,
        case
            when o.order_status is null then false
            {% if excluded_statuses %}
            when o.order_status in (
                {% for status in excluded_statuses %}
                '{{ status | replace("'", "''") }}'{% if not loop.last %},{% endif %}
                {% endfor %}
            ) then false
            {% endif %}
            else true
        end as is_revenue_eligible,
        i._batch_date,
        i._event_id,
        i._source_file,
        i._loaded_at,
        greatest(i._loaded_at, coalesce(o._loaded_at, i._loaded_at)) as _source_loaded_at
    from {{ ref('stg_order_items') }} i
    left join {{ ref('stg_orders') }} o on i.order_id = o.order_id
    left join {{ ref('stg_products') }} p on i.product_id = p.product_id
    left join {{ ref('int_customer_history') }} h
        on o.customer_id = h.customer_id
        and o.order_placed_at >= h.valid_from
        and (o.order_placed_at < h.valid_to or h.is_current)
)

select
    *,
    (quantity * unit_price)::number(18, 2) as gross_line_amount,
    (quantity * unit_cost)::number(18, 2) as line_cost,
    (line_total - quantity * unit_cost)::number(18, 2) as line_margin,
    case when is_revenue_eligible then line_total else 0 end::number(18, 2) as revenue_amount
from joined
