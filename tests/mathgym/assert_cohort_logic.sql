-- Fails if any user's subscription_cohort is inconsistent with their lifecycle dates.
-- Validates the CASE statement in int_mathgym__user_subscription_status.

select app_user_id, subscription_cohort, paid_date, paid_cancelled_date, trial_cancelled_date
from {{ ref('int_mathgym__user_subscription_status') }}
where
    -- active_paid must have paid_date and no paid_cancelled_date
    (subscription_cohort = 'active_paid' and (paid_date is null or paid_cancelled_date is not null))
    or
    -- paid_churned must have both paid_date and paid_cancelled_date
    (subscription_cohort = 'paid_churned' and (paid_date is null or paid_cancelled_date is null))
    or
    -- trial_churned must NOT have paid_date, must have trial_cancelled_date
    (subscription_cohort = 'trial_churned' and (paid_date is not null or trial_cancelled_date is null))
    or
    -- trial_active must NOT have paid_date, must NOT have trial_cancelled_date
    (subscription_cohort = 'trial_active' and (paid_date is not null or trial_cancelled_date is not null))
