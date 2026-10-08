select
    o.customer_unique_id,
    count(*) as review_count,
    round(avg(r.review_score), 2) as avg_review_score,
    round(safe_divide(countif(r.review_score <= 2), count(*)), 4) as low_score_ratio,
    round(safe_divide(countif(r.review_comment_message is not null), count(*)), 4) as comment_ratio,
    round(avg(length(r.review_comment_message)), 1) as avg_comment_length
from {{ source('silver', 'order_reviews') }} as r
join {{ ref('stg_orders') }} as o
    on r.order_id = o.order_id
group by o.customer_unique_id
