select
    category,
    count(distinct order_id) as orders,
    count(distinct customer_id) as customers,
    sum(quantity) as units,
    round(sum(revenue), 2) as revenue,
    round(sum(revenue) / count(distinct order_id), 2) as average_order_value,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from silver_orders
group by category
order by revenue desc, category;
