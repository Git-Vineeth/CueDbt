-- Grain: one row per payment transaction (after junk invoice filter).
-- Attribution joined from mbt_leads via student_service_id → parent_service_id.
-- Replaces data_playground.mbt_payments_2024 (Periscope DROP+INSERT script).

{{
    config(
        materialized='table',
        sort='paid_on',
        dist='payment_id'
    )
}}

with payments as (

    select * from {{ ref('int_marketing__payments_enriched') }}

),

leads as (

    -- One lead per student (latest) for attribution join
    select *
    from (
        select
            *,
            row_number() over (
                partition by student_service_id
                order by created_date desc
            ) as rnk
        from {{ ref('mbt_leads') }}
        where student_service_id is not null
    ) t
    where rnk = 1

)

select
    p.payment_id,
    p.student_id,
    p.student_service_id,
    p.parent_service_id,
    p.product,
    p.invoice_id,
    p.paid_on,
    p.paid_on::date                         as paid_date,
    date_trunc('month', p.paid_on)::date    as paid_month,
    p.amount,
    p.amount_usd,
    p.net_amount,
    p.base_amount,
    p.discount_amount,
    p.refund_flag,
    p.refund_amount,
    p.currency,
    p.currency_conversion_rate,
    p.grade,
    p.enrollment_type,
    p.enrollment_type_new,
    p.enrollment_type_final,
    p.prev_enrollment_type,
    p.payment_type,
    p.payment_rank,
    p.fees_type,
    p.region,
    p.billing_country,
    p.agent_email,
    p.teacher_id,
    p.coupon_code,
    p.plan_id,
    p.state,

    -- Attribution from mbt_leads
    l.channel,
    l.channel_ref,
    l.utm_medium_normalized                 as utm_medium,
    l.utm_campaign,
    l.lead_type,
    l.grade_slab                            as lead_grade_slab,
    l.crm_source

from payments p
left join leads l
    on p.student_service_id = l.student_service_id
