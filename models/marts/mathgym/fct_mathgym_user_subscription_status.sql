-- Grain: one row per user. Source of truth for all per-user MathGym subscription analysis.
-- PK: app_user_id

{{
    config(materialized='table', dist='app_user_id')
}}

select
    app_user_id,
    subscription_cohort,
    is_family_plan,
    trial_start_date::date                          as trial_start_date,
    paid_date::date                                 as paid_date,
    trial_cancelled_date::date                      as trial_cancelled_date,
    paid_cancelled_date::date                       as paid_cancelled_date,
    paid_cancel_reason,
    paid_cancel_reason = 'UNSUBSCRIBE'              as is_voluntary_churn,
    trial_to_paid_days,
    paid_tenure_days,
    to_char(trial_start_date, 'YYYY-MM')            as trial_start_month,
    channel,
    channel_ref,
    utm_medium,
    referral_type,
    lead_type,
    os,
    grade,
    lead_region

from {{ ref('int_mathgym__user_subscription_status') }}
