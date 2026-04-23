-- Grain: one row per subscription lifecycle event per real user.
-- Applies two mandatory exclusions:
--   1. SANDBOX events (Apple/Google test purchases — never real revenue)
--   2. Test users — any app_user_id with any event before 2026-02-11 (MathGym launch).
--      ALL their events are excluded, not just the pre-launch ones.

with test_users as (

    select distinct app_user_id
    from {{ ref('stg_revenuecat__events') }}
    where created_on < '2026-02-11'::date
      and event_type is not null

),

cleaned as (

    select
        rce.app_user_id,
        rce.event_type,
        rce.period_type,
        rce.price_usd,
        rce.is_family_plan,
        rce.cancel_reason,
        rce.created_on,
        rce.created_on_date,
        rce.created_on_month

    from {{ ref('stg_revenuecat__events') }} rce
    where rce.environment != 'SANDBOX'
      and rce.created_on >= '2026-02-11'::date
      and not exists (
          select 1 from test_users tu where tu.app_user_id = rce.app_user_id
      )

)

select * from cleaned
