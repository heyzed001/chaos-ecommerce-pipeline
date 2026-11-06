{% set calendar = date_dimension_bounds() %}
{% set months = ['January', 'February', 'March', 'April', 'May', 'June',
                 'July', 'August', 'September', 'October', 'November', 'December'] %}
{% set weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'] %}

with calendar as (
    -- ROW_NUMBER supplies gap-free offsets; SEQ4 alone can contain gaps.
    -- The rowcount is rendered as an integer constant before Snowflake runs.
    select dateadd(
        day,
        row_number() over (order by seq4()) - 1,
        '{{ calendar.start_date }}'::date
    )::date as full_date
    from table(generator(rowcount => {{ calendar.day_count }}))
)

select
    to_char(full_date, 'YYYYMMDD')::integer as date_key,
    full_date,
    dayofmonth(full_date)::integer as day_of_month,
    dayofyear(full_date)::integer as day_of_year,
    month(full_date)::integer as month,
    case month(full_date)
        {% for name in months %}
        when {{ loop.index }} then '{{ name }}'
        {% endfor %}
    end as month_name,
    quarter(full_date)::integer as quarter,
    year(full_date)::integer as year,
    dayofweekiso(full_date)::integer as day_of_week_iso,
    case dayofweekiso(full_date)
        {% for name in weekdays %}
        when {{ loop.index }} then '{{ name }}'
        {% endfor %}
    end as day_of_week,
    weekiso(full_date)::integer as iso_week,
    yearofweekiso(full_date)::integer as iso_year,
    dayofweekiso(full_date) in (6, 7) as is_weekend,
    date_trunc('month', full_date)::date as month_start_date,
    last_day(full_date, 'month')::date as month_end_date,
    date_trunc('quarter', full_date)::date as quarter_start_date,
    date_trunc('year', full_date)::date as year_start_date,
    full_date = last_day(full_date, 'month') as is_month_end,
    (mod(year(full_date), 400) = 0
        or (mod(year(full_date), 4) = 0 and mod(year(full_date), 100) != 0)) as is_leap_year
from calendar
