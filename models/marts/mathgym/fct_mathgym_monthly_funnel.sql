-- Grain: one row per month (funnel steps 1-12).
-- Steps 1-3 use source_guest_id (pre-signup). Steps 4-12 use user_id (post-signup).
-- Steps 13-15 (trial/paid/cancellation) are in fct_mathgym_monthly_subscription_summary.
-- Test users excluded via campaign = 'account_test'.

{{
    config(materialized='table')
}}

with funnel_events as (

    select * from {{ ref('stg_events__mathgym_funnel') }}
    where campaign is null or campaign != 'account_test'

),

monthly as (

    select
        event_month,

        count(distinct case when event_name = 'first_app_open'
              then source_guest_id end)                                                 as step_1_app_opens,
        count(distinct case when event_name = 'app_signup_cta_clicked'
              then source_guest_id end)                                                 as step_2_signup_cta_clicked,
        count(distinct case when event_name = 'app_parent_created'
              then source_guest_id end)                                                 as step_3_parent_created,

        count(distinct case when event_name = 'app_signup_step_viewed'
                             and signup_step_name = 'select-grade'
              then user_id end)                                                         as step_4_grade_selected,
        count(distinct case when event_name = 'app_signup_step_viewed'
                             and signup_step_name = 'select-child-math-skills'
              then user_id end)                                                         as step_5_math_skills_viewed,
        count(distinct case when event_name = 'app_signup_step_viewed'
                             and signup_step_name = 'select-child-goals'
              then user_id end)                                                         as step_6_goals_viewed,
        count(distinct case when event_name = 'app_signup_step_viewed'
                             and signup_step_name = 'create-username'
              then user_id end)                                                         as step_7_username_viewed,

        -- NULL != 'enrolled' evaluates to NULL in Redshift — IS NULL must be explicit
        count(distinct case when event_name = 'app_child_account_created'
                             and (student_enrolment_status in ('unenrolled', 'undefined')
                                  or student_enrolment_status is null)
              then user_id end)                                                         as step_8_child_account_created,
        count(distinct case when event_name = 'circle_onboarding_completed'
                             and student_enrolment_status = 'unenrolled'
              then user_id end)                                                         as step_9_onboarding_completed,
        count(distinct case when event_name = 'activity_started'
              then user_id end)                                                         as step_10_activity_started,
        count(distinct case when event_name = 'mathgym_paywall_screen_viewed'
              then user_id end)                                                         as step_11_paywall_viewed,
        count(distinct case when event_name = 'mathgym_paywall_subscription_payment_initiated'
              then user_id end)                                                         as step_12_payment_initiated

    from funnel_events
    group by 1

)

select
    event_month                                                                         as month,
    step_1_app_opens,
    step_2_signup_cta_clicked,
    step_3_parent_created,
    step_4_grade_selected,
    step_5_math_skills_viewed,
    step_6_goals_viewed,
    step_7_username_viewed,
    step_8_child_account_created,
    step_9_onboarding_completed,
    step_10_activity_started,
    step_11_paywall_viewed,
    step_12_payment_initiated,
    round(100.0 * step_2_signup_cta_clicked    / nullif(step_1_app_opens, 0), 2)       as pct_cta_clicked,
    round(100.0 * step_3_parent_created        / nullif(step_1_app_opens, 0), 2)       as pct_parent_created,
    round(100.0 * step_8_child_account_created / nullif(step_1_app_opens, 0), 2)       as pct_child_created,
    round(100.0 * step_10_activity_started     / nullif(step_1_app_opens, 0), 2)       as pct_activity_started,
    round(100.0 * step_12_payment_initiated    / nullif(step_1_app_opens, 0), 2)       as pct_payment_initiated

from monthly
