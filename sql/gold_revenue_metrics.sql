select
    order_date,
    category,
    count(*) as orders,
    sum(quantity) as units,
    sum(quantity * unit_price) as revenue,
    sum(quantity * unit_price) / count(*) as average_order_value
from silver_orders
where status = 'delivered'
group by order_date, category
order by order_date, category;
