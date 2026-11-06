{# Parquet dates may arrive as epoch days and timestamps as epoch integers
   when logical-type conversion is disabled in the Snowflake file format. #}
{% macro source_date(expression) -%}
    case
        when typeof({{ expression }}) in ('INTEGER', 'DECIMAL', 'DOUBLE')
            then dateadd(day, {{ expression }}::integer, '1970-01-01'::date)::date
        else try_to_date({{ expression }}::varchar)
    end
{%- endmacro %}

{% macro source_timestamp(expression) -%}
    {# Snowflake infers seconds/milliseconds/microseconds/nanoseconds from
       integer strings; formatted timestamp strings are also supported. #}
    try_to_timestamp_ntz({{ expression }}::varchar)
{%- endmacro %}
