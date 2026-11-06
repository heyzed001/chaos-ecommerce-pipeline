{% set calendar = date_dimension_bounds() %}

select
    count(*) as row_count,
    count(distinct full_date) as distinct_days,
    min(full_date) as first_day,
    max(full_date) as last_day
from {{ ref('dim_date') }}
having row_count != {{ calendar.day_count }}
    or distinct_days != {{ calendar.day_count }}
    or first_day != '{{ calendar.start_date }}'::date
    or last_day != '{{ calendar.end_date }}'::date
