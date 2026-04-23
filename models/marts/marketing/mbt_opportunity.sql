-- Grain: one row per Superleap CRM opportunity (post-Apr 2026 era).
-- is_workable_unique = 1 is the canonical record per student for funnel counting.
-- Replaces the existing mbt_opportunity table built from Periscope/cron scripts.

{{
    config(
        materialized='table',
        sort='created_date',
        dist='opportunity_id'
    )
}}

select
    opportunity_id,
    student_service_id,
    parent_service_id,
    created_at_ist,
    created_date,
    created_month,
    stage,
    state,
    closure_reason,
    grade,
    grade_slab          as grade_slab,
    country,
    region,
    sub_region,
    ethnicity,
    source_channel_raw,
    channel,
    channel_ref,
    utm_medium,
    utm_medium_normalized,
    sales_process,
    is_unqualified,
    is_close_type1,
    is_workable,
    is_workable_unique,
    'Superleap'         as crm_source

from {{ ref('int_marketing__superleap_enriched') }}
