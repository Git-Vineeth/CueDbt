-- Grain: one row per region (NAM / ISC / ROW). Lifetime, not time-bounded.
-- adoption_rate = students who completed >= 1 test / all students assigned >= 1 test.

{{
    config(materialized='table')
}}

select
    region,
    count(student_id)                                                               as total_students_assigned,
    count(case when mft_completed >= 1 then student_id end)                        as students_adopted,
    round(
        count(case when mft_completed >= 1 then student_id end)::decimal(10, 2)
        / nullif(count(student_id), 0),
        2
    )                                                                               as adoption_rate

from {{ ref('int_mathfit_tests__student_lifetime') }}
group by 1
