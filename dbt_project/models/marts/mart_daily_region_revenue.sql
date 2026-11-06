select
    order_date_key,
    order_date,
    order_currency,
    customer_country,
    customer_region,
    {{ revenue_summary_columns() }}
from {{ ref('revenue_line_items') }}
group by order_date_key, order_date, order_currency, customer_country, customer_region
