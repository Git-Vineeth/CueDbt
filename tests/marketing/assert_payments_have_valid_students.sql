{{ config(severity='warn') }}

-- Warns if any payment has no matching lead in mbt_leads.
-- Known gap: direct/offline enrollments and pre-tracking-era students bypass the lead funnel.

select p.payment_id, p.student_service_id
from {{ ref('mbt_payments') }} p
left join {{ ref('mbt_leads') }} l
    on p.student_service_id = l.student_service_id
where l.student_service_id is null
  and p.student_service_id is not null
