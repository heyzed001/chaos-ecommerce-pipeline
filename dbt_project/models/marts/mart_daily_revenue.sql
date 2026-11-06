select
    order_date_key,
    order_date,
    order_currency,
    {{ revenue_summary_columns() }}
from {{ ref('revenue_line_items') }}
group by order_date_key, order_date, order_currency
