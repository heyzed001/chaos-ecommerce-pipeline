select
    (select count(*) from {{ ref('stg_order_items') }}) as source_count,
    (select count(*) from {{ ref('int_order_items_enriched') }}) as enriched_count
where source_count != enriched_count
