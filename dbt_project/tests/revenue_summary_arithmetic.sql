select order_date_key, order_currency
from {{ ref('mart_daily_revenue') }}
where gross_sales_amount - discount_amount != revenue_amount
    or (cost_amount is not null and revenue_amount - cost_amount != margin_amount)
    or revenue_amount < 0
    or units_sold < 0
    or eligible_item_count > observed_item_count
    or (missing_or_invalid_cost_item_count > 0 and (cost_amount is not null or margin_amount is not null))
