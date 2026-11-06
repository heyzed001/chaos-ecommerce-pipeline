{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_item_id',
    on_schema_change='fail'
) }}

{% if is_incremental() %}
with watermark as (
    select
        coalesce(max(_batch_date), '1900-01-01'::date) as latest_batch_date,
        coalesce(max(_source_loaded_at), '1900-01-01'::timestamp_ltz) as latest_source_load
    from {{ this }}
)
{% endif %}

select
    e.order_item_id,
    e.order_id,
    e.line_number,
    c.customer_key,
    p.product_key,
    d.date_key as order_date_key,
    e.customer_id,
    e.product_id,
    e.order_placed_at,
    e.order_date,
    e.order_status,
    e.order_currency,
    e.payment_method,
    e.quantity,
    e.unit_price,
    e.unit_cost,
    e.discount_amount,
    e.line_total,
    e.gross_line_amount,
    e.line_cost as cost_amount,
    e.line_margin as margin_amount,
    e.revenue_amount,
    e.is_revenue_eligible,
    e.is_order_missing,
    e.is_product_missing,
    e.is_customer_history_missing,
    e.is_quantity_invalid,
    e.is_unit_cost_missing,
    e._batch_date,
    e._event_id,
    e._source_file,
    e._source_loaded_at
from {{ ref('int_order_items_enriched') }} e
left join {{ ref('dim_product') }} p on e.product_id = p.product_id
left join {{ ref('dim_customer') }} c on e.customer_version_id = c.customer_version_id
left join {{ ref('dim_date') }} d on e.order_date = d.full_date
{% if is_incremental() %}
cross join watermark
left join {{ this }} previous on e.order_item_id = previous.order_item_id
{% endif %}
where {{ fact_incremental_filter() }}
