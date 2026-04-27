-- Grain: one row per qualified LSQ lead (pre-Apr 2026 era).
-- Joins stg_lsq__lead_data + stg_lsq__leads_test + stg_app__user_source_log + stg_app__parent_profile.
-- Applies channel_attribution and landingpage_grouping dbt macros.
-- Filters: removes unqualified, coding, SPQB, offline, and test leads.
-- This model owns all LSQ-era attribution logic — no Periscope macros needed.

with lead_data as (

    select * from {{ ref('stg_lsq__lead_data') }}

),

leads_test as (

    select * from {{ ref('stg_lsq__leads_test') }}

),

user_tracking as (

    select * from {{ ref('stg_app__user_source_log') }}

),

parent_profile as (

    select * from {{ ref('stg_app__parent_profile') }}

),

enriched as (

    select
        l.lead_id,
        l.student_service_id,
        l.parent_service_id,
        l.student_id,
        l.parent_id,

        -- Dates: referral date takes priority over CRM creation date
        l.referral_type                                          as referral_type_raw,
        coalesce(
            case when l.referral_type is not null
                 then {{ utc_to_ist('l.created_at_utc') }}
            end,
            {{ utc_to_ist('l.created_at_utc') }}
        )                                                        as created_at_ist,
        coalesce(
            case when l.referral_type is not null
                 then l.created_at_utc::date
            end,
            l.created_date
        )                                                        as created_date,

        -- Demographics
        l.grade,
        l.country,
        l.country_segments,
        l.ethnicity,
        l.priority_regions,

        -- CRM stage & funnel flags
        l.stage,
        l.qualified_bucket,
        l.cna_completed,
        l.pac_completed,
        l.demo_scheduled,
        l.demo_done,
        l.enrolled,

        -- Attribution inputs
        l.utm_source,
        l.utm_medium,
        l.utm_campaign,
        l.detail_source,
        l.source,
        l.app_utm,
        l.utm_ts,
        l.apps_ts,
        l.channel_old,

        -- Referral signals
        l.referral_type,
        l.ref_status,

        -- Latest CRM fields from leads_test
        t.product_type,
        t.owner_name,
        t.is_teacher_led,
        t.calendly_time_slot,
        coalesce(t.ethnicity_crm, l.ethnicity)                  as ethnicity_final,

        -- Landing page (priority: CRM category → session tracking → signup landing)
        coalesce(
            u.session_landing_page,
            p.landing_page
        )                                                        as landing_page_raw,

        -- Landing page category (replaces [landingpage_grouping] Periscope macro)
        {{ landingpage_grouping('coalesce(u.session_landing_page, p.landing_page)') }}
                                                                 as landing_type,

        -- City (student tracking → parent signup fallback)
        coalesce(u.ip_city, p.ip_city)                          as ip_city,

        -- Device
        p.device_type,
        coalesce(u.os, p.os_type)                               as os,

        -- Channel attribution (replaces [channel_test_referral_logic] Periscope macro)
        {{ channel_attribution('l.utm_source', 'l.source', 'l.detail_source', 'l.referral_type') }}
                                                                 as channel_ref,

        -- UTM medium normalised (replaces inline CASE blocks)
        {{ utm_medium_grouping('l.utm_medium', 'l.utm_source') }}
                                                                 as utm_medium_normalized

    from lead_data l
    left join leads_test t
        on l.lead_id = t.lead_id
    left join user_tracking u
        on l.student_service_id = u.student_service_id
    left join parent_profile p
        on l.parent_id = p.parent_id

),

qualified_only as (

    select * from enriched
    where lower(qualified_bucket) not like '%unqual%'
      and lower(qualified_bucket) not like '%coding%'
      and lower(qualified_bucket) not like '%spqb%'
      and lower(qualified_bucket) not like '%offline%'
      and lower(coalesce(lead_id, '')) not like '%test%'
      and created_date >= '{{ var("marketing_start_date") }}'

)

select * from qualified_only
