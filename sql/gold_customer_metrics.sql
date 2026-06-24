select
    customer_id,
    count(distinct order_id) as orders,
    sum(quantity) as units,
    round(sum(revenue), 2) as revenue,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from silver_orders
group by customer_id
order by revenue desc, customer_id;
