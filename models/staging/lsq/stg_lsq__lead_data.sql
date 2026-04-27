-- Grain: one row per lead (latest CRM state).
-- Source: public.lsq_lead_data (masked view over thirdparty_lsq.lsq_lead_data).
-- Phone and email are already masked at source — do not attempt to unmask.
-- This model selects and renames the fields needed downstream.
-- No business logic here — channel_ref and final channel are computed in intermediate.

with source as (

    select * from {{ source('lsq', 'lead_data') }}

),

renamed as (

    select
        prospectid                                              as lead_id,
        student_service_id,
        parent_service_id,
        parent_id,
        student_id,
        student_name,
        firstname                                               as parent_first_name,
        lastname                                                as parent_last_name,

        createdon                                               as created_at_utc,
        {{ utc_to_ist('createdon') }}                           as created_at_ist,
        createdon::date                                         as created_date,
        date_trunc('month', createdon)::date                    as created_month,
        modifiedon                                              as updated_at_utc,

        -- CRM stage
        prospectstage                                           as stage,
        qualified_bucket,
        qualified_leads,
        auto_manual_tag,

        -- Demographics
        grade,
        grade_new,
        country_new                                             as country,
        country_segments,
        ethnicity,
        priority_regions,

        -- Funnel milestones
        demo_scheduled,
        demo_done,
        enrolled,
        demo_confirmed_new,
        demo_scheduled_mtd,
        cna_completed,
        cna_not_completed,
        pac_completed,
        mx_cna_done_timestamp,
        mx_plus_pac_done_timestamp,

        -- Demo scheduling
        mx_demo_date,
        mx_demo_scheduled_timstamp                              as demo_scheduled_at,
        mx_demo_done_timestamp                                  as demo_done_at,
        paid_date,

        -- Attribution
        mx_utm_source                                           as utm_source,
        mx_utm_medium                                           as utm_medium,
        mx_utm_campaign                                         as utm_campaign,
        mx_utm_adcontent                                        as utm_adcontent,
        detail_source,
        source,
        origin,
        app_utm,
        utm_ts,
        apps_ts,
        channel_old,
        channel                                                 as channel_source,  -- pre-computed in source, kept for reference

        -- Referral
        mx_referral_type                                        as referral_type,
        ref_status,
        ref_country,
        mx_wildfire_ts                                          as wildfire_at,
        wildfire_student,

        -- Lead metadata
        product,
        mx_lead_type,
        mx_lead_type_new,
        mx_lead_segment,
        mx_lead_channel,
        mx_tenure,
        duplicate_flag,
        duplicate_new,

        -- CRM ownership
        mx_lead_owner                                           as lead_owner,
        mx_demo_owner                                           as demo_owner,
        mx_payment_owner                                        as payment_owner,
        mx_rescheduling_owner                                   as rescheduling_owner,
        scheduling_owner,

        -- Location
        mx_lead_city                                            as lead_city,
        mx_lead_ip_city                                         as ip_city,
        mx_school_city                                          as school_city,

        -- CRM tracking
        mx_auto_demo_scheduled,
        mx_is_phone_verified,
        new_definition_paid_unpaid,

        -- ITM params (alternative to UTM)
        itm_source,
        itm_medium,
        itm_campaign

    from source

)

select * from renamed
