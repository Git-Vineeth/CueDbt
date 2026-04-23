-- Grain: one row per student (lifetime, not time-bounded).
-- Aggregates all MathFit tests per student.
-- Joins stg_intelenrollment__student for region.
-- Coalesces region to 'ROW' if student not in intelenrollment (data gap safeguard).

with lifetime as (

    select
        student_id,
        count(distinct mathfit_test_id)                                                     as count_mft_assigned,
        count(distinct case when test_status = 'FULL_COMPLETION' then mathfit_test_id end)  as mft_completed
    from {{ ref('int_mathfit_tests__student_test_status') }}
    group by 1

)

select
    l.student_id,
    coalesce(s.region, 'ROW')   as region,
    l.count_mft_assigned,
    l.mft_completed
from lifetime l
left join {{ ref('stg_intelenrollment__student') }} s
    on l.student_id = s.student_id
