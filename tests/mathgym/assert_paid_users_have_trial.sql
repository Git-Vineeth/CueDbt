-- Fails if any user has a paid_date but no trial_start_date.
-- MathGym always requires a trial before paid conversion.
-- If this fires: RevenueCat out-of-sequence, identity mismatch, or test user escaped filter.

select app_user_id
from {{ ref('int_mathgym__user_subscription_status') }}
where paid_date is not null
  and trial_start_date is null
