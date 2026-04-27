-- Grain: one row per subscription lifecycle event per user.
-- Source: application_service_intelenrollment.revenue_cat_events_log
-- NOTE: SANDBOX events are NOT filtered here — filter in intermediate.
-- Extracts SUPER type fields from raw_payload as top-level columns.

with source as (

    select * from {{ source('revenuecat', 'revenue_cat_events_log') }}

),

renamed as (

    select
        app_user_id,
        event_type,
        period_type,
        environment,
        price_usd,

        -- Extracted from SUPER type raw_payload (cast to text first — json_extract_path_text does not accept SUPER directly)
        json_extract_path_text(raw_payload::text, 'is_family_share', true)::boolean
                                                               as is_family_plan,
        json_extract_path_text(raw_payload::text, 'cancel_reason', true)::text
                                                               as cancel_reason,

        created_on,
        created_on::date                                       as created_on_date,
        date_trunc('month', created_on)::date                  as created_on_month

    from source

)

select * from renamed
