select
    rejection_reason,
    status,
    order_date,
    category,
    count(distinct order_id) as rejected_orders,
    sum(quantity) as rejected_units,
    round(sum(quantity * unit_price), 2) as potential_revenue
from {{ source('lakehouse', 'rejected_orders') }}
group by rejection_reason, status, order_date, category
