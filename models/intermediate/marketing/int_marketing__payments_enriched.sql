-- Grain: one row per payment transaction.
-- Adds enrollment type normalisation, previous enrollment type (lag), and payment type logic.
-- Trial upgrade payments are re-labelled to prevent polluting true renewal counts.

with payments as (

    select * from {{ ref('stg_payments__payment') }}

),

with_enrollment_type as (

    select
        *,

        -- Normalise enrollment type
        -- Trial payments mis-labelled as PLUS/REGULAR renewals are re-labelled TRIAL_UPGRADE
        case
            when enrollment_type_new in ('PLUS', 'REGULAR')
                 and fees_type = 'renewal'
                 and (
                     json_extract_path_text(meta, 'is_trial_upgrade_payment', true) = 'true'
                     or (
                         json_extract_path_text(meta, 'is_upgrade_v2_payment', true) = 'true'
                         and invoice_id like '%upgrade_v2%'
                     )
                 )                                                               then 'TRIAL_UPGRADE'
            when enrollment_type_new is null
                 or enrollment_type_new = ''                                     then 'REGULAR'
            else enrollment_type_new
        end                                                                      as enrollment_type_final,

        -- Previous enrollment type (for upgrade/downgrade detection)
        lag(enrollment_type_new) over (
            partition by student_id, product
            order by paid_on
        )                                                                        as prev_enrollment_type

    from payments

),

with_region as (

    select
        *,

        -- Normalised region from derived_region
        case
            when lower(derived_region) like '%us%'      then 'US'
            when lower(derived_region) like '%row%'     then 'ROW'
            else                                             'India'
        end                                                                      as region,

        -- Amount in USD
        round(amount::numeric * currency_conversion_rate::numeric, 2)            as amount_usd,

        -- Gross vs net booking
        case
            when refund_flag = true then amount - refund_amount
            else amount
        end                                                                      as net_amount

    from with_enrollment_type

)

select * from with_region
