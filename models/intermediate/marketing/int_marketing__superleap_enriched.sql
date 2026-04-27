-- Grain: one row per Superleap CRM opportunity (post-Apr 2026 era).
-- Applies Superleap channel mapping and workable/dedup logic.
-- is_workable_unique = 1 marks the canonical record per student for funnel counting.

with opportunities as (

    select * from {{ ref('stg_superleap__opportunities') }}

),

with_channel as (

    select
        *,

        -- source_channel is already human-readable (e.g. 'Perf Meta', 'Perf Google', 'Referral')
        -- Normalise to canonical channel buckets used across LSQ and Superleap eras
        case
            when source_channel_raw ilike '%organic%'                       then 'Organic'
            when source_channel_raw ilike '%perf meta%'
              or source_channel_raw ilike '%perf_meta%'                     then 'Perf_meta'
            when source_channel_raw ilike '%perf google%'
              or source_channel_raw ilike '%perf_google%'                   then 'Perf_google'
            when source_channel_raw ilike '%perf%'                          then 'Perf_others'
            when source_channel_raw ilike '%referral%'                      then 'Referrals'
            when source_channel_raw ilike '%app%'                           then 'App'
            else                                                                  'Others'
        end                                                                  as channel,

        -- Simplified channel_ref aligned with revenue_channel codes
        case
            when revenue_channel = 'REFERRAL'                               then 'Referrals'
            when revenue_channel = 'PAID_AD'                                then 'Performance'
            when revenue_channel = 'ORGANIC'                                then 'Organic'
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
            partition by student_id_crm
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
