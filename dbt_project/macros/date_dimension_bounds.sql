{% macro date_dimension_bounds() %}
    {% set start_text = var('dim_date_start_date', '2024-01-01') | string %}
    {% set end_text = var('dim_date_end_date', '2028-12-31') | string %}
    {% for value in [start_text, end_text] %}
        {% if not modules.re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2}', value) %}
            {{ exceptions.raise_compiler_error('dim_date bounds must be YYYY-MM-DD dates') }}
        {% endif %}
    {% endfor %}
    {% set start_date = modules.datetime.datetime.strptime(start_text, '%Y-%m-%d').date() %}
    {% set end_date = modules.datetime.datetime.strptime(end_text, '%Y-%m-%d').date() %}
    {% if end_date < start_date %}
        {{ exceptions.raise_compiler_error('dim_date_end_date must be on or after dim_date_start_date') }}
    {% endif %}
    {{ return({'start_date': start_text, 'end_date': end_text,
               'day_count': (end_date - start_date).days + 1}) }}
{% endmacro %}
