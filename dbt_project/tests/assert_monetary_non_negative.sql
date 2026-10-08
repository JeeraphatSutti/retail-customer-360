-- singular test: returns rows that violate the rule (test passes when 0 rows)
select customer_unique_id, monetary
from {{ ref('customer_360') }}
where monetary < 0 or recency_days < 0
