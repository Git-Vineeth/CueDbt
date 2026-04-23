-- Grain: one row per MathGym funnel event per user.
-- Source: event_analytics.events (hundreds of millions of rows — incremental is mandatory).
-- Replaces the daily cron DROP + SELECT INTO data_playground.mathgym_funnel_events.
-- Incremental strategy: append new events by event_ts_ist since last run.
-- First run: processes all events since mathgym_launch_date (2026-02-11).

{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='append'
    )
}}

with source as (

    select * from {{ source('event_analytics', 'events') }}

    where event_name in (
        'first_app_open',
        'app_signup_cta_clicked',
        'app_parent_created',
        'app_signup_step_viewed',
        'app_child_account_created',
        'circle_onboarding_completed',
        'activity_started',
        'mathgym_paywall_screen_viewed',
        'mathgym_paywall_subscription_payment_initiated',
        'mathgym_paywall_subscription_payment_success',
        'mathgym_subscription_trial_converted'
    )
    and event_ts_ist::date >= '{{ var("mathgym_launch_date") }}'

    {% if is_incremental() %}
        and event_ts_ist > (select max(event_ts_ist) from {{ this }})
    {% endif %}

),

renamed as (

    select
        event_id,
        event_name,
        attributes.source_guest_id::varchar                    as source_guest_id,
        user_id,
        country,
        event_ts_ist,
        event_ts_ist::date                                     as event_date,
        date_trunc('month', event_ts_ist)::date                as event_month,

        -- Step 4-7 distinguisher: which signup step was viewed?
        attributes.app_signup_step_name::text                  as signup_step_name,

        -- Enrolment status filter for steps 8-9
        attributes.student_enrolment_status::text              as student_enrolment_status,

        -- Test user identifier (account_test = internal)
        attributes.campaign::text                              as campaign

    from source

)

select * from renamed
