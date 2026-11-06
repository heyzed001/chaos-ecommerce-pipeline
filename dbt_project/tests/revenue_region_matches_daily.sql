with regional as (
    select
        order_date_key,
        order_currency,
        sum(observed_item_count) as observed_item_count,
        sum(eligible_item_count) as eligible_item_count,
        sum(units_sold) as units_sold,
        sum(revenue_amount) as revenue_amount,
        case when sum(missing_or_invalid_cost_item_count) > 0 then null
            else sum(cost_amount) end as cost_amount,
        case when sum(missing_or_invalid_cost_item_count) > 0 then null
            else sum(margin_amount) end as margin_amount
    from {{ ref('mart_daily_region_revenue') }}
    group by order_date_key, order_currency
)

select coalesce(d.order_date_key, r.order_date_key) as order_date_key
from {{ ref('mart_daily_revenue') }} d
full outer join regional r
    on d.order_date_key = r.order_date_key and d.order_currency = r.order_currency
where d.order_date_key is null or r.order_date_key is null
    or d.observed_item_count is distinct from r.observed_item_count
    or d.eligible_item_count is distinct from r.eligible_item_count
    or d.units_sold is distinct from r.units_sold
    or d.revenue_amount is distinct from r.revenue_amount
    or d.cost_amount is distinct from r.cost_amount
    or d.margin_amount is distinct from r.margin_amount
