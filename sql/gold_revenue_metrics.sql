select
    order_date,
    category,
    count(distinct order_id) as orders,
    sum(quantity) as units,
    round(sum(revenue), 2) as revenue,
    round(sum(revenue) / count(distinct order_id), 2) as average_order_value
from silver_orders
group by order_date, category
order by order_date, category;
