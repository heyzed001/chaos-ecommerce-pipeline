with ordered as (
    select
        *,
        lag(full_date) over (order by full_date) as previous_day
    from {{ ref('dim_date') }}
)

select *
from ordered
where (previous_day is not null and datediff(day, previous_day, full_date) != 1)
    or date_key != year(full_date) * 10000 + month(full_date) * 100 + dayofmonth(full_date)
    or (day_of_week_iso in (6, 7)) != is_weekend
    or month_start_date > full_date
    or month_end_date < full_date
    or dayofmonth(month_start_date) != 1
    or (is_month_end != (full_date = month_end_date))
    or (is_leap_year and month(full_date) = 2 and dayofmonth(month_end_date) != 29)
    or (not is_leap_year and month(full_date) = 2 and dayofmonth(month_end_date) != 28)
