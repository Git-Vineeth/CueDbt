-- Grain: one row per month. All percentages out of total_students_mft_assigned.

{{
    config(materialized='table')
}}

with monthly as (

    select
        created_on_month,
        count(distinct student_id)                                                                      as total_students_mft_assigned,
        count(distinct case when test_status = 'NOT_STARTED'                    then student_id end)    as students_no_sections_started,
        count(distinct case when test_status in (
                                    'IN_PROGRESS', 'PARTIAL_COMPLETION', 'FULL_COMPLETION'
                                )                                               then student_id end)    as students_sections_started,
        count(distinct case when test_status = 'IN_PROGRESS'                    then student_id end)    as students_started_not_completed_any,
        count(distinct case when test_status = 'PARTIAL_COMPLETION'             then student_id end)    as students_partial_completion,
        count(distinct case when test_status = 'FULL_COMPLETION'                then student_id end)    as students_all_sections_done,
        cast(avg(case when test_status = 'FULL_COMPLETION' then overall_score end) as decimal(10, 2))   as avg_overall_score

    from {{ ref('int_mathfit_tests__student_test_status') }}
    group by 1

)

select
    created_on_month                                                                                        as month,
    total_students_mft_assigned,
    students_no_sections_started,
    students_sections_started,
    students_started_not_completed_any,
    students_partial_completion,
    students_all_sections_done,
    round(100.0 * students_no_sections_started / nullif(total_students_mft_assigned, 0), 2)     as not_engaged_pct,
    round(100.0 * students_sections_started    / nullif(total_students_mft_assigned, 0), 2)     as engaged_pct,
    round(100.0 * students_all_sections_done   / nullif(total_students_mft_assigned, 0), 2)     as completion_pct_out_of_total_assigned,
    avg_overall_score

from monthly
