-- Fails if any payment has a student_service_id with no matching lead in mbt_leads.
-- Every paying student must have come through the lead funnel.

select p.payment_id, p.student_service_id
from {{ ref('mbt_payments') }} p
left join {{ ref('mbt_leads') }} l
    on p.student_service_id = l.student_service_id
where l.student_service_id is null
  and p.student_service_id is not null
