-- Grain: one row per user (lifetime).
-- Pivots event-level data to user-level. Derives subscription_cohort and lifecycle dates.
-- Joins mbt_leads mart for marketing attribution (cross-domain dependency — intentional).
-- ROW_NUMBER deduplication: trial = most recent, paid = earliest, cancellations = most recent.

with latest_lead as (

    -- mbt_leads can have multiple rows per parent_service_id (a parent may have multiple leads).
    -- Pick the most recently created lead per parent for attribution.
    select *
    from (
        select
            *,
            row_number() over (
                partition by parent_service_id
                order by created_date desc
            ) as rnk
        from {{ ref('mbt_leads') }}
        where parent_service_id is not null
    ) t
    where rnk = 1

),

trial as (

    select app_user_id, created_on as trial_start_date, is_family_plan
    from (
        select
            app_user_id,
            created_on,
            is_family_plan,
            row_number() over (
                partition by app_user_id
                order by created_on desc
            ) as rnk
        from {{ ref('int_mathgym__subscription_events') }}
        where event_type  = 'INITIAL_PURCHASE'
          and period_type = 'TRIAL'
    ) t
    where rnk = 1

),

paid as (

    select app_user_id, created_on as paid_date
    from (
        select
            app_user_id,
            created_on,
            row_number() over (
                partition by app_user_id
                order by created_on asc
            ) as rnk
        from {{ ref('int_mathgym__subscription_events') }}
        where event_type  in ('INITIAL_PURCHASE', 'RENEWAL')
          and period_type != 'TRIAL'
          and price_usd    > 0
    ) p
    where rnk = 1

),

trial_cancelled as (

    select app_user_id, created_on as trial_cancelled_date
    from (
        select
            app_user_id,
            created_on,
            row_number() over (
                partition by app_user_id
                order by created_on desc
            ) as rnk
        from {{ ref('int_mathgym__subscription_events') }}
        where event_type  = 'CANCELLATION'
          and period_type = 'TRIAL'
    ) tc
    where rnk = 1

),

paid_cancelled as (

    -- cancel_reason = 'UNSUBSCRIBE' = voluntary churn. Exposed as a column for mart-level analysis.
    select app_user_id, created_on as paid_cancelled_date, cancel_reason
    from (
        select
            app_user_id,
            created_on,
            cancel_reason,
            row_number() over (
                partition by app_user_id
                order by created_on desc
            ) as rnk
        from {{ ref('int_mathgym__subscription_events') }}
        where event_type  = 'CANCELLATION'
          and period_type = 'NORMAL'
    ) pc
    where rnk = 1

),

labeled as (

    select
        t.app_user_id,
        t.trial_start_date,
        t.is_family_plan,
        p.paid_date,
        tc.trial_cancelled_date,
        pc.paid_cancelled_date,
        pc.cancel_reason                                                        as paid_cancel_reason,

        -- Mutually exclusive cohort label
        case
            when p.app_user_id  is null and tc.app_user_id is not null         then 'trial_churned'
            when p.app_user_id  is not null and pc.app_user_id is not null     then 'paid_churned'
            when p.app_user_id  is not null and pc.app_user_id is null         then 'active_paid'
            else                                                                     'trial_active'
        end                                                                     as subscription_cohort,

        datediff('day', t.trial_start_date::date, p.paid_date::date)           as trial_to_paid_days,
        datediff('day', p.paid_date::date, pc.paid_cancelled_date::date)        as paid_tenure_days

    from trial t
    left join paid p             on t.app_user_id = p.app_user_id
    left join trial_cancelled tc on t.app_user_id = tc.app_user_id
    left join paid_cancelled pc  on t.app_user_id = pc.app_user_id

)

select
    l.app_user_id,
    l.subscription_cohort,
    l.is_family_plan,
    l.trial_start_date,
    l.paid_date,
    l.trial_cancelled_date,
    l.paid_cancelled_date,
    l.paid_cancel_reason,
    l.trial_to_paid_days,
    l.paid_tenure_days,
    ml.channel,
    ml.channel_ref,
    ml.utm_medium_normalized                                                    as utm_medium,
    ml.referral_type,
    ml.lead_type,
    ml.os,
    ml.grade,
    ml.region                                                                   as lead_region
from labeled l
left join latest_lead ml
    on l.app_user_id = ml.parent_service_id
