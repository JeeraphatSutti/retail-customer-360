with latest_profile as (
    select customer_unique_id, customer_city, customer_state, customer_zip_code_prefix
    from {{ ref('stg_orders') }}
    qualify row_number() over (
        partition by customer_unique_id order by order_purchase_timestamp desc
    ) = 1
)

select
    rfm.customer_unique_id,
    prof.customer_city,
    prof.customer_state,
    prof.customer_zip_code_prefix,
    rfm.recency_days,
    rfm.frequency,
    rfm.monetary,
    rfm.first_order_ts,
    rfm.last_order_ts,
    rfm.r_score,
    rfm.f_score,
    rfm.m_score,
    rfm.rfm_segment,
    rev.review_count,
    rev.avg_review_score,
    rev.low_score_ratio,
    rev.comment_ratio,
    rev.avg_comment_length,
    bp.avg_basket_size,
    bp.avg_item_price,
    bp.total_freight,
    bp.freight_to_price_ratio,
    bp.avg_installments,
    bp.max_installments,
    bp.credit_card_share,
    bp.boleto_share,
    bp.voucher_share,
    bp.debit_card_share,
    current_timestamp() as feature_generated_at
from {{ ref('customer_rfm') }} as rfm
left join latest_profile as prof using (customer_unique_id)
left join {{ ref('customer_review_features') }} as rev using (customer_unique_id)
left join {{ ref('customer_basket_payment_features') }} as bp using (customer_unique_id)
