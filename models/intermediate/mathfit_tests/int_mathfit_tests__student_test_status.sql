-- Grain: one row per student per test.
-- Aggregates section-level states from int_mathfit_tests__section_detail.
-- Status priority: FULL_COMPLETION > PARTIAL_COMPLETION > IN_PROGRESS > NOT_STARTED.

with section_counts as (

    select
        mathfit_test_id,
        student_id,
        created_on_month,
        count(distinct section_number)                                                   as sections_assigned,
        count(distinct case when section_status = 'COMPLETED'   then section_number end) as sections_completed,
        count(distinct case when section_status = 'IN_PROGRESS' then section_number end) as sections_in_progress,
        count(distinct case when section_status = 'NOT_STARTED' then section_number end) as sections_not_started,
        count(distinct case when section_status = 'LOCKED'      then section_number end) as sections_locked,
        sum(section_score)                                                               as total_section_score,
        max(overall_score)                                                               as overall_score

    from {{ ref('int_mathfit_tests__section_detail') }}
    group by 1, 2, 3

)

select
    mathfit_test_id,
    student_id,
    created_on_month,
    sections_assigned,
    sections_completed,
    sections_in_progress,
    sections_not_started,
    sections_locked,
    total_section_score,
    case
        when sections_completed = sections_assigned
             and sections_assigned > 0                          then overall_score
        else                                                         null
    end                                                         as overall_score,
    case
        when sections_completed = sections_assigned
             and sections_assigned > 0                          then 'FULL_COMPLETION'
        when sections_completed >= 1
             and sections_completed < sections_assigned         then 'PARTIAL_COMPLETION'
        when sections_in_progress >= 1
             and sections_completed = 0                         then 'IN_PROGRESS'
        else                                                         'NOT_STARTED'
    end                                                         as test_status
from section_counts
