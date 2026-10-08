with order_value as (
    select order_id, sum(payment_value) as order_value
    from {{ source('silver', 'order_payments') }}
    group by order_id
),

base as (
    select
        o.customer_unique_id,
        o.order_id,
        o.order_purchase_timestamp,
        coalesce(v.order_value, 0) as order_value
    from {{ ref('stg_orders') }} as o
    left join order_value as v on o.order_id = v.order_id
),

ref_date as (
    select max(order_purchase_timestamp) as ref_ts from base
),

agg as (
    select
        customer_unique_id,
        count(distinct order_id) as frequency,
        round(sum(order_value), 2) as monetary,
        min(order_purchase_timestamp) as first_order_ts,
        max(order_purchase_timestamp) as last_order_ts
    from base
    group by customer_unique_id
),

scored as (
    select
        a.*,
        date_diff(date(r.ref_ts), date(a.last_order_ts), day) as recency_days,
        -- 4 = most recent / highest spend
        ntile(4) over (order by date_diff(date(r.ref_ts), date(a.last_order_ts), day) desc) as r_score,
        case when a.frequency >= 3 then 3 when a.frequency = 2 then 2 else 1 end as f_score,
        ntile(4) over (order by a.monetary) as m_score
    from agg as a
    cross join ref_date as r
)

select
    *,
    case
        when r_score >= 3 and f_score >= 2 then 'loyal'
        when r_score >= 3 and m_score >= 3 then 'new_high_value'
        when r_score >= 3 then 'recent'
        when m_score >= 3 then 'at_risk_high_value'
        else 'lapsed'
    end as rfm_segment
from scored
