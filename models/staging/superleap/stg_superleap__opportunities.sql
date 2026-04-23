-- Grain: one row per CRM opportunity (post-Apr 2026 Superleap era).
-- Source: thirdparty_crm_superleap.opportunity
-- Filters: is_deleted = false, type = 'Tutoring', created_on >= 2026-02-01.
-- Channel is computed from source_channel in intermediate, not here.

with source as (

    select * from {{ source('superleap', 'opportunity') }}
    where is_deleted = false
      and type = 'Tutoring'
      and created_on >= '2026-02-01'

),

renamed as (

    select
        opportunity_id,
        student_service_id,
        parent_service_id,

        -- Timestamps (IST-adjusted: created_on is UTC, +330 min = IST)
        created_on                                              as created_at_utc,
        {{ utc_to_ist('created_on') }}                          as created_at_ist,
        created_on::date                                        as created_date,
        date_trunc('month', {{ utc_to_ist('created_on') }})::date as created_month,

        -- CRM fields
        stage,
        state,
        closure_reason,

        -- Demographics
        grade::int                                              as grade,
        country,
        region                                                  as region_raw,

        -- Normalised region
        case
            when region = 'IND'  then 'India'
            when region = 'US'   then 'US'
            else                      'ROW'
        end                                                     as region,

        sub_region,
        ethnicity,

        -- Attribution (raw — channel computed in intermediate)
        source_channel                                          as source_channel_raw,
        utm_medium,
        sales_process

    from source

)

select * from renamed
