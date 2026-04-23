-- Fails if any user's paid_date is before their trial_start_date.
-- Chronological violation indicates duplicate/merged accounts or RevenueCat sync issue.

select app_user_id, trial_start_date, paid_date
from {{ ref('int_mathgym__user_subscription_status') }}
where paid_date is not null
  and trial_start_date is not null
  and paid_date::date < trial_start_date::date
