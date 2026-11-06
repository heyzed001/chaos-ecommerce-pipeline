-- Every observed customer must have exactly one open version.
select customer_id
from {{ ref('int_customer_history') }}
group by customer_id
having coalesce(count_if(is_current), 0) != 1
