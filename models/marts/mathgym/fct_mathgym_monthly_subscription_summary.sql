-- Grain: one row per trial_start_month cohort.
-- PM dashboard — subscription volume, conversion, and churn by cohort.
-- All rates: 0-100 scale, 2 decimal places. NULLIF prevents divide-by-zero.

{{
    config(materialized='table')
}}

with monthly as (

    select
        to_char(trial_start_date, 'YYYY-MM')                                                    as month,
        count(distinct app_user_id)                                                              as total_trials,
        count(distinct case when subscription_cohort = 'trial_active'   then app_user_id end)   as trials_still_active,
        count(distinct case when subscription_cohort = 'trial_churned'  then app_user_id end)   as trials_churned,
        count(distinct case when subscription_cohort in ('active_paid', 'paid_churned')
                            then app_user_id end)                                                as total_converted,
        count(distinct case when subscription_cohort = 'active_paid'    then app_user_id end)   as active_paid,
        count(distinct case when subscription_cohort = 'paid_churned'   then app_user_id end)   as paid_churned,
        count(distinct case when subscription_cohort = 'paid_churned'
                             and paid_cancel_reason  = 'UNSUBSCRIBE'    then app_user_id end)   as voluntary_churned,
        count(distinct case when is_family_plan = true                  then app_user_id end)   as family_plan_users,
        count(distinct case when is_family_plan = false                 then app_user_id end)   as individual_plan_users,
        cast(avg(trial_to_paid_days)  as decimal(10, 1))                                        as avg_days_to_convert,
        cast(avg(paid_tenure_days)    as decimal(10, 1))                                        as avg_paid_tenure_days

    from {{ ref('int_mathgym__user_subscription_status') }}
    group by 1

)

select
    month,
    total_trials,
    trials_still_active,
    trials_churned,
    total_converted,
    active_paid,
    paid_churned,
    voluntary_churned,
    family_plan_users,
    individual_plan_users,
    avg_days_to_convert,
    avg_paid_tenure_days,
    round(100.0 * total_converted   / nullif(total_trials,    0), 2)    as trial_to_paid_rate,
    round(100.0 * trials_churned    / nullif(total_trials,    0), 2)    as trial_churn_rate,
    round(100.0 * paid_churned      / nullif(total_converted, 0), 2)    as paid_churn_rate,
    round(100.0 * voluntary_churned / nullif(paid_churned,    0), 2)    as voluntary_churn_pct_of_paid_churned,
    round(100.0 * family_plan_users / nullif(total_converted, 0), 2)    as family_plan_pct

from monthly
