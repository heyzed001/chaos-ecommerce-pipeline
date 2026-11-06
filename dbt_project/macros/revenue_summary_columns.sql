{% macro revenue_summary_columns() %}
    count(*) as observed_item_count,
    count(distinct case when is_reportable then order_id end) as order_count,
    sum(case when is_reportable then 1 else 0 end) as eligible_item_count,
    sum(case when not is_revenue_eligible then 1 else 0 end) as excluded_status_item_count,
    sum(case when not is_line_valid then 1 else 0 end) as invalid_item_count,
    sum(case when is_customer_history_missing then 1 else 0 end) as missing_customer_history_item_count,
    sum(case when is_reportable and not is_cost_valid then 1 else 0 end) as missing_or_invalid_cost_item_count,
    sum(case when is_reportable then quantity else 0 end) as units_sold,
    sum(case when is_reportable then quantity * unit_price else 0 end)::number(18, 2) as gross_sales_amount,
    sum(case when is_reportable then discount_amount else 0 end)::number(18, 2) as discount_amount,
    sum(case when is_reportable then line_total else 0 end)::number(18, 2) as revenue_amount,
    case when sum(case when is_reportable and not is_cost_valid then 1 else 0 end) > 0 then null
        else sum(case when is_reportable then cost_amount else 0 end)
    end::number(18, 2) as cost_amount,
    case when sum(case when is_reportable and not is_cost_valid then 1 else 0 end) > 0 then null
        else sum(case when is_reportable then line_total - cost_amount else 0 end)
    end::number(18, 2) as margin_amount
{% endmacro %}
