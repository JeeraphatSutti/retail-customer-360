with basket as (
    select
        order_id,
        count(*) as items_in_order,
        sum(price) as items_value,
        sum(freight_value) as freight_value
    from {{ source('silver', 'order_items') }}
    group by order_id
),

basket_by_customer as (
    select
        o.customer_unique_id,
        round(avg(b.items_in_order), 2) as avg_basket_size,
        round(safe_divide(sum(b.items_value), sum(b.items_in_order)), 2) as avg_item_price,
        round(sum(b.freight_value), 2) as total_freight,
        round(safe_divide(sum(b.freight_value), sum(b.items_value)), 4) as freight_to_price_ratio
    from {{ ref('stg_orders') }} as o
    join basket as b on o.order_id = b.order_id
    group by o.customer_unique_id
),

payment_by_customer as (
    select
        o.customer_unique_id,
        round(avg(p.payment_installments), 2) as avg_installments,
        max(p.payment_installments) as max_installments,
        round(safe_divide(sum(if(p.payment_type = 'credit_card', p.payment_value, 0)), sum(p.payment_value)), 4) as credit_card_share,
        round(safe_divide(sum(if(p.payment_type = 'boleto', p.payment_value, 0)), sum(p.payment_value)), 4) as boleto_share,
        round(safe_divide(sum(if(p.payment_type = 'voucher', p.payment_value, 0)), sum(p.payment_value)), 4) as voucher_share,
        round(safe_divide(sum(if(p.payment_type = 'debit_card', p.payment_value, 0)), sum(p.payment_value)), 4) as debit_card_share
    from {{ source('silver', 'order_payments') }} as p
    join {{ ref('stg_orders') }} as o on p.order_id = o.order_id
    group by o.customer_unique_id
)

select
    coalesce(b.customer_unique_id, p.customer_unique_id) as customer_unique_id,
    b.avg_basket_size,
    b.avg_item_price,
    b.total_freight,
    b.freight_to_price_ratio,
    p.avg_installments,
    p.max_installments,
    p.credit_card_share,
    p.boleto_share,
    p.voucher_share,
    p.debit_card_share
from basket_by_customer as b
full outer join payment_by_customer as p
    on b.customer_unique_id = p.customer_unique_id
