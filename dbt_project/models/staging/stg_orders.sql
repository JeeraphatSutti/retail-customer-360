-- orders joined to the real customer (customer_unique_id), excluding unusable orders
select
    o.order_id,
    c.customer_unique_id,
    o.order_status,
    o.order_purchase_timestamp,
    c.customer_city,
    c.customer_state,
    c.customer_zip_code_prefix
from {{ source('silver', 'orders') }} as o
join {{ source('silver', 'customers') }} as c
    on o.customer_id = c.customer_id
where o.order_status not in ('canceled', 'unavailable')
