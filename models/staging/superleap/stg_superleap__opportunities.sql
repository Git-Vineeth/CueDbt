-- Grain: one row per CRM opportunity (post-Apr 2026 Superleap era).
-- Source: thirdparty_crm_superleap.opportunity
-- Filters: is_deleted = false, type = 'Tutoring', created_on >= 2026-02-01.
-- Channel is already human-readable in source_channel; revenue_channel has raw codes.

with source as (

    select * from {{ source('superleap', 'opportunity') }}
    where is_deleted = false
      and type = 'Tutoring'
      and created_on >= '2026-02-01'

),

renamed as (

    select
        id                                                      as opportunity_id,
        record_id,

        -- CRM identity (Superleap internal IDs — join to app via related_usl_id)
        associated_student                                      as student_id_crm,
        associated_parent                                       as parent_id_crm,
        associated_product                                      as product_id_crm,
        related_usl_id,                                         -- application-layer UUID

        -- Timestamps (IST-adjusted: created_on is UTC)
        created_on                                              as created_at_utc,
        {{ utc_to_ist('created_on') }}                          as created_at_ist,
        created_on::date                                        as created_date,
        date_trunc('month', {{ utc_to_ist('created_on') }})::date as created_month,
        updated_on                                              as updated_at_utc,
        attribution_opportunity_created_on,

        -- CRM stage / qualification
        stage,
        state,
        opportunity_state,
        is_qualified,
        unqualified_reason,
        closure_reason,
        close_reason,
        closed_ts,
        expires_on,

        -- Funnel milestones
        trial_scheduled,
        trial_attended,
        trial_datetime,
        last_trial_scheduled_on,
        last_trial_done_on,
        event_checkedin_date,
        follow_up_date,

        -- Payment info
        paid_amount,
        paid_currency,
        payment_date,
        classes_purchased,
        paid_tenure,

        -- Demographics
        grade::int                                              as grade,
        country,
        region                                                  as region_raw,

        case
            when region = 'IND'                                then 'India'
            when region = 'USA'                                then 'US'
            else                                                    'ROW'
        end                                                     as region,

        school_board,

        -- Attribution (source_channel is already human-readable in this source)
        source_channel                                          as source_channel_raw,
        revenue_channel,                                        -- raw code: ORGANIC / PAID_AD / REFERRAL / OTHER
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        utm_term_new,
        sales_process,
        update_source,

        -- CRM ownership / ops
        owner                                                   as owner_name,
        owner_id,
        cuemath_created_by,
        created_by,
        updated_by,
        slot_calling_rnr_counter,
        payment_calling_rnr_counter,
        eligible_for_auto_mapping,

        -- Quality / outcome tracking
        what_went_well,
        what_went_wrong,
        not_interested_reason,
        not_interested_disposition,
        not_interested_sub_disposition,
        not_interested_remarks,
        violation_titles,

        meta                                                    as meta_raw

    from source

)

select * from renamed
