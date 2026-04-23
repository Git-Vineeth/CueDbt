-- Grain: one row per student per month. Student-level engagement for cohort and individual reporting.

{{
    config(materialized='table', dist='student_id')
}}

select
    sts.student_id,
    sts.created_on_month                            as month,
    sts.mathfit_test_id,
    sts.sections_assigned,
    sts.sections_completed,
    sts.sections_in_progress,
    sts.sections_not_started,
    sts.sections_locked,
    sts.test_status,
    sts.overall_score,
    case when sts.test_status = 'NOT_STARTED'       then false else true  end   as is_engaged,
    case when sts.test_status = 'FULL_COMPLETION'   then true  else false end   as is_completed,
    sd.total_time_spent_seconds

from {{ ref('int_mathfit_tests__student_test_status') }} sts
left join (
    select
        mathfit_test_id,
        student_id,
        sum(section_time_spent)     as total_time_spent_seconds
    from {{ ref('int_mathfit_tests__section_detail') }}
    group by 1, 2
) sd
    on sts.mathfit_test_id = sd.mathfit_test_id
    and sts.student_id     = sd.student_id
