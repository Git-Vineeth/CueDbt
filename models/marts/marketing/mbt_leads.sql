-- Grain: one row per qualified lead.
-- Dual-source: LSQ (pre-Apr 2026) UNION Superleap (post-Apr 2026).
-- Replaces data_playground.mbt_leads (Periscope DROP+INSERT script).
-- Channel attribution is computed by dbt macros — no Periscope runtime macros needed.

{{
    config(
        materialized='table',
        sort='created_date',
        dist='lead_id'
    )
}}

-- Pre-compute grade as integer to avoid Redshift optimizer evaluating grade::int
-- on non-numeric values (e.g. 'K' for Kindergarten) before the CASE condition is checked
with lsq_raw as (

    select
        *,
        case when grade ~ '^[0-9]+$' then grade::int else null end as grade_int
    from {{ ref('int_marketing__lsq_enriched') }}
    where created_date < '{{ var("superleap_cutover_date") }}'

),

lsq_leads as (

    select
        lead_id,
        parent_service_id,
        student_service_id,
        student_id,
        parent_id,
        created_at_ist,
        created_date,
        date_trunc('month', created_date)::date                 as created_month,
        grade,
        country,
        country_segments,
        ethnicity_final                                          as ethnicity,
        stage,
        qualified_bucket,
        cna_completed::varchar,
        pac_completed::varchar,
        demo_scheduled::varchar,
        demo_done::varchar,
        enrolled::varchar,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_medium_normalized,
        channel_ref,
        landing_type,
        ip_city,
        os,
        device_type,
        product_type,
        owner_name,
        is_teacher_led::varchar,
        referral_type,
        ref_status,

        -- grade_int is already null-safe (null when non-numeric)
        case
            when grade_int between 9 and 12    then '4. High'
            when grade_int between 6 and 8     then '3. 6-8'
            when grade_int between 3 and 5     then '2. 3-5'
            else                                    '1. K-2'
        end                                                     as grade_slab,

        -- Region
        case
            when lower(qualified_bucket) like '%us%'            then 'US'
            when lower(qualified_bucket) like '%row%'           then 'ROW'
            else                                                     'India'
        end                                                     as region,

        -- Lead type
        case
            when lower(qualified_bucket) not like '%us%'
                 and lower(qualified_bucket) not like '%row%'
                 and (is_teacher_led = 1
                      or lower(utm_source) like '%teacher%')    then 'T-Led'
            else                                                     'C-Led'
        end                                                     as lead_type,

        -- Final channel (combines channel_ref + utm_medium + landing_type)
        case
            when channel_ref = 'Referrals'                                          then 'Referrals'
            when channel_ref = 'Performance'
                 and utm_medium_normalized = 'google_brand'                         then 'Perf_google_brand'
            when channel_ref = 'Performance'
                 and utm_medium_normalized = 'google_other'                         then 'Perf_google_other'
            when channel_ref = 'Performance'
                 and utm_medium_normalized = 'meta'                                 then 'Perf_meta'
            when channel_ref = 'Performance'                                        then 'Perf_others'
            when channel_ref = 'Organic'
                 and landing_type = 'Content'                                       then 'Organic Content'
            when channel_ref = 'Organic'
                 and landing_type = 'Intent'                                        then 'Organic Intent'
            when channel_ref = 'Organic'
                 and landing_type = 'Brand'                                         then 'Organic Brand'
            when channel_ref = 'Organic'                                            then 'Organic Non-Content'
            else                                                                         'Others'
        end                                                     as channel,

        'LSQ'                                                   as crm_source

    from lsq_raw

),

superleap_leads as (

    select
        opportunity_id                                          as lead_id,
        parent_service_id,
        student_service_id,
        null::varchar                                           as student_id,
        null::varchar                                           as parent_id,
        created_at_ist,
        created_date,
        created_month,
        grade::varchar                                          as grade,
        country,
        null::varchar                                           as country_segments,
        ethnicity,
        stage,
        null::varchar                                           as qualified_bucket,
        null::varchar                                           as cna_completed,
        null::varchar                                           as pac_completed,
        null::varchar                                           as demo_scheduled,
        null::varchar                                           as demo_done,
        null::varchar                                           as enrolled,
        null::varchar                                           as utm_source,
        utm_medium,
        null::varchar                                           as utm_campaign,
        utm_medium_normalized,
        channel_ref,
        null::varchar                                           as landing_type,
        null::varchar                                           as ip_city,
        null::varchar                                           as os,
        null::varchar                                           as device_type,
        null::varchar                                           as product_type,
        null::varchar                                           as owner_name,
        null::varchar                                           as is_teacher_led,
        null::varchar                                           as referral_type,
        null::varchar                                           as ref_status,

        case
            when grade between 9 and 12                         then '4. High'
            when grade between 6 and 8                          then '3. 6-8'
            when grade between 3 and 5                          then '2. 3-5'
            else                                                     '1. K-2'
        end                                                     as grade_slab,

        region,

        case
            when channel_ref = 'Organic'
                 and source_channel_raw = 'ORGANIC_TEACHER'     then 'T-Led'
            else                                                     'C-Led'
        end                                                     as lead_type,

        channel,

        'Superleap'                                             as crm_source

    from {{ ref('int_marketing__superleap_enriched') }}
    where created_date >= '{{ var("superleap_cutover_date") }}'

),

unified as (

    select * from lsq_leads
    union all
    select * from superleap_leads

)

select * from unified
