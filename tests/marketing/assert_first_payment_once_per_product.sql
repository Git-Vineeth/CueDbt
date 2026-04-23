-- Fails if a (student_id, product) combination has more than one 'first' payment.
-- fees_type = 'first' should fire exactly once per student per product.

select student_id, product, count(*) as first_payment_count
from {{ ref('mbt_payments') }}
where fees_type = 'first'
group by 1, 2
having count(*) > 1
