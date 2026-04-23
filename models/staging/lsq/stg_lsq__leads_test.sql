-- Grain: one row per lead (latest CRM update — deduplicated).
-- Source: thirdparty_lsq.leads_test2 — multiple rows per lead, one per CRM field update.
-- Deduplication: ROW_NUMBER() over prospectid ordered by modifiedon DESC.
-- Provides product_type, owner name, mx_ethinicity not available in stg_lsq__lead_data.

with source as (

    select * from {{ source('thirdparty_lsq', 'leads_test') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by prospectid
            order by modifiedon desc
        ) as row_num

    from source

),

latest as (

    select * from deduped where row_num = 1

),

renamed as (

    select
        prospectid                                              as lead_id,
        modifiedon                                              as updated_at_utc,

        -- Product type (normalised)
        case
            when lower(product_type) in ('hybrid', 'class_k5')  then 'Hybrid'
            when product_type is null                            then 'Plus'
            when lower(product_type) in ('base', 'base')        then 'Base'
            when lower(product_type) = 'high'                   then 'High'
            else product_type
        end                                                     as product_type,

        -- CRM ownership
        owneridname                                             as owner_name,

        -- Ethnicity from CRM (separate from ethnicity in lsq_lead_data)
        mx_ethinicity                                           as ethnicity_crm,

        -- Additional CRM fields
        mx_tled                                                 as is_teacher_led,
        mx_calendly_time_slot                                   as calendly_time_slot

    from latest

)

select * from renamed
