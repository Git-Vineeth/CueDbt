-- Grain: one row per Superleap CRM opportunity (post-Apr 2026 era).
-- Applies Superleap channel mapping and workable/dedup logic.
-- is_workable_unique = 1 marks the canonical record per student for funnel counting.

with opportunities as (

    select * from {{ ref('stg_superleap__opportunities') }}

),

with_channel as (

    select
        *,

        -- Superleap channel mapping (simpler than LSQ — source_channel is already clean)
        case
            when source_channel_raw in ('ORGANIC', 'ORGANIC_INBOUND',
                                         'ORGANIC_DIRECT', 'ORGANIC_BRAND') then 'Organic Non-Content'
            when source_channel_raw = 'ORGANIC_TEACHER'                     then 'TEACHER'
            when source_channel_raw in ('PAID_AD_FACEBOOK', 'PAID_AD_FB')   then 'Perf_meta'
            when source_channel_raw = 'PAID_AD_GOOGLE'                      then 'Perf_google'
            when source_channel_raw in ('REFERRAL_PARENT', 'REFERRAL_SALES',
                                         'REFERRAL_STUDENT', 'REFERRAL_TEACHER')
                                                                             then 'Referrals'
            when source_channel_raw in ('PAID_AD', 'PAID_AD_AFFILIATE')     then 'Perf_others'
            else                                                                  'Others'
        end                                                                  as channel,

        -- Simplified channel_ref for Superleap (no 4-macro chain needed)
        case
            when source_channel_raw like 'REFERRAL%'                        then 'Referrals'
            when source_channel_raw like 'PAID_AD%'                         then 'Performance'
            when source_channel_raw like 'ORGANIC%'                         then 'Organic'
            else                                                                  'Others'
        end                                                                  as channel_ref,

        -- UTM medium bucket
        {{ utm_medium_grouping('utm_medium', 'source_channel_raw') }}        as utm_medium_normalized,

        -- Workable flags
        0                                                                    as is_unqualified,
        case
            when state = 'Closed'
                 and closure_reason in ('Active', 'New')                    then 1
            else 0
        end                                                                  as is_close_type1

    from opportunities

),

with_workable as (

    select
        *,
        case
            when is_unqualified = 0 and is_close_type1 = 0                  then 1
            else 0
        end                                                                  as is_workable

    from with_channel

),

with_dedup as (

    select
        *,
        row_number() over (
            partition by student_service_id
            order by
                case when state = 'Open' then 0 else 1 end,
                created_at_utc desc
        )                                                                    as workable_rank

    from with_workable
    where is_workable = 1

)

select
    o.*,
    case when w.workable_rank = 1 then 1 else 0 end                          as is_workable_unique
from with_workable o
left join with_dedup w
    on o.opportunity_id = w.opportunity_id
