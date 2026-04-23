-- Grain: one row per payment transaction (after junk invoice filter).
-- Source: data_models.payment
-- Junk invoice filter: removes test/ops invoices by scanning invoice_id patterns.
-- Exception: dummy_uae_n% invoices are REAL UAE payments and must be kept.
-- payment_rank and fees_type derived here to avoid re-computing downstream.

with source as (

    select * from {{ source('payments', 'payment') }}

),

invoice_filtered as (

    select * from source
    where (
        lower(invoice_id) not like '%dummy%'
        and lower(invoice_id) not like '%test%'
        and lower(invoice_id) not like '%dup%'
        and lower(invoice_id) not like '%can%'
        and lower(invoice_id) not like '%cls_transfer%'
        and lower(invoice_id) not like '%cls_extension%'
        and lower(invoice_id) not like '%tets%'
        and lower(invoice_id) not like '%stuent_up_%'
        and lower(invoice_id) not like '%upgrade_v2%'
        and lower(invoice_id) not like '%free%'
        and invoice_id not like '%US_CONTRACT%'
    )
    or invoice_id is null
    or lower(invoice_id) like '%dummy_uae_n%'

),

with_ranks as (

    select
        payment_id,
        student_id,
        student_service_id,
        parent_service_id,
        product,
        invoice_id,
        paid_on,
        paid_on_original,
        amount,
        base_amount,
        discount_amount,
        refund_flag,
        refund_amount,
        refund_date,
        duration,
        currency,
        currency_conversion_rate,
        grade,
        own_discount,
        source,
        course,
        plan_id,
        class_ratio,
        no_classes,
        coupon_code,
        enrollment_type,
        enrollment_type_new,
        derived_region,
        billing_country,
        agent_email,
        country_code,
        teacher_id,
        agent_name,
        state,
        validity_till_date,
        meta,

        -- Is this a subscription payment?
        case
            when coalesce(len(json_extract_path_text(meta, 'subscription_id', true)), 0) = 0
            then 'normal'
            else 'subscription'
        end                                                     as payment_type,

        -- Rank within (student, product) by paid_on ASC — rank 1 = first ever payment
        row_number() over (
            partition by student_id, product
            order by paid_on asc
        )                                                       as payment_rank

    from invoice_filtered

)

select
    *,
    case when payment_rank = 1 then 'first' else 'renewal' end  as fees_type
from with_ranks
