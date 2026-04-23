-- Fails if any app_user_id appears more than once in the user subscription status model.
-- Duplicates here cause double-counting in every mathgym mart model.

select app_user_id, count(*) as row_count
from {{ ref('int_mathgym__user_subscription_status') }}
group by 1
having count(*) > 1
