{% macro fact_incremental_filter() %}
    {% set lookback = var('late_arrival_lookback_days', 8) %}
    {% if lookback is not integer or lookback < 0 %}
        {{ exceptions.raise_compiler_error('late_arrival_lookback_days must be a non-negative integer') }}
    {% endif %}
    {% if is_incremental() %}
        previous.order_item_id is null
        or e._batch_date >= dateadd(day, -{{ lookback }}, watermark.latest_batch_date)
        or e._source_loaded_at >= watermark.latest_source_load
        or previous.customer_key is distinct from c.customer_key
        or previous.product_key is distinct from p.product_key
        or previous.order_date_key is distinct from d.date_key
        or previous.is_revenue_eligible is distinct from e.is_revenue_eligible
    {% else %}
        true
    {% endif %}
{% endmacro %}
