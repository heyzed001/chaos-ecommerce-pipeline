{% macro customer_snapshot_date() -%}
    {% set batch_date = var('customer_snapshot_date', none) %}
    {% if batch_date is not none %}
        {% set parsed = modules.datetime.datetime.strptime(batch_date | string, '%Y-%m-%d') %}
        {% if parsed.strftime('%Y-%m-%d') != batch_date | string %}
            {{ exceptions.raise_compiler_error('customer_snapshot_date must be YYYY-MM-DD') }}
        {% endif %}
    {% endif %}
    {{ return(batch_date) }}
{%- endmacro %}

{% macro customer_snapshot_rows() %}
    {% set batch_date = customer_snapshot_date() %}
    select
        nullif(trim(_record:customer_id::varchar), '') as customer_id,
        nullif(trim(_record:first_name::varchar), '') as first_name,
        nullif(trim(_record:last_name::varchar), '') as last_name,
        nullif(lower(trim(_record:email::varchar)), '') as email,
        nullif(trim(_record:phone::varchar), '') as phone,
        nullif(trim(_record:address_line::varchar), '') as address_line,
        nullif(trim(_record:city::varchar), '') as city,
        nullif(trim(_record:region::varchar), '') as region,
        nullif(trim(_record:country::varchar), '') as country,
        nullif(trim(_record:postal_code::varchar), '') as postal_code,
        {{ source_date('_record:signup_date') }} as signup_date,
        lower(trim(_record:customer_segment::varchar)) as customer_segment,
        {{ source_timestamp('_record:updated_at') }} as updated_at
    from {{ source('raw', 'customers') }}
    {% if batch_date is not none %}
    where _batch_date = '{{ batch_date }}'::date
    {% endif %}
    qualify row_number() over (
        partition by customer_id
        order by {{ source_timestamp('_record:updated_at') }} desc nulls last,
            _batch_date desc, _event_id desc
    ) = 1
{% endmacro %}

{% macro validate_customer_snapshot_input() %}
    {# Run as a snapshot pre-hook, before any snapshot merge. #}
    {% if execute %}
        {% set validation_sql %}
            with incoming as ({{ customer_snapshot_rows() }})
            select count(*) as rows_found,
                coalesce(count_if(customer_id is null or updated_at is null), 0) as invalid_rows,
                count(*) - count(distinct customer_id) as duplicate_keys
            from incoming
        {% endset %}
        {% set validation = run_query(validation_sql) %}
        {% if validation.rows[0][0] == 0 %}
            {{ exceptions.raise_compiler_error('No customer rows for snapshot input. Load the requested batch first.') }}
        {% endif %}
        {% if (validation.rows[0][1] or 0) > 0 or (validation.rows[0][2] or 0) > 0 %}
            {{ exceptions.raise_compiler_error('Snapshot input has missing keys/timestamps or duplicate customers.') }}
        {% endif %}
        {% set existing = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
        {% if existing is not none %}
            {% set backward_sql %}
                with incoming as ({{ customer_snapshot_rows() }})
                select count(*)
                from incoming i
                join {{ this }} h on i.customer_id = h.customer_id
                where h.dbt_valid_to is null and i.updated_at < h.dbt_updated_at
            {% endset %}
            {% if run_query(backward_sql).rows[0][0] > 0 %}
                {{ exceptions.raise_compiler_error('Snapshot input moves customer versions backwards. Replay oldest to newest into a fresh snapshot; existing history was not reset.') }}
            {% endif %}
        {% endif %}
    {% endif %}
    {{ return('select 1') }}
{% endmacro %}
