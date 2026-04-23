-- Grain: one row per section_title per month. Excludes LOCKED sections (never reachable).

{{
    config(materialized='table')
}}

with section_base as (

    select
        created_on_month,
        section_title,
        section_number,
        section_status,
        section_score,
        section_accuracy,
        total_questions,
        correct_answers,
        student_id,
        mathfit_test_id

    from {{ ref('int_mathfit_tests__section_detail') }}
    where section_status != 'LOCKED'
      and section_title is not null

)

select
    created_on_month,
    section_title,
    section_number,
    count(distinct mathfit_test_id || '-' || student_id)                        as total_sections_assigned,
    count(distinct case when section_status = 'COMPLETED'
                        then mathfit_test_id || '-' || student_id end)          as sections_completed,
    count(distinct case when section_status = 'IN_PROGRESS'
                        then mathfit_test_id || '-' || student_id end)          as sections_in_progress,
    count(distinct case when section_status = 'NOT_STARTED'
                        then mathfit_test_id || '-' || student_id end)          as sections_not_started,
    round(100.0 * count(distinct case when section_status = 'COMPLETED'
                                      then mathfit_test_id || '-' || student_id end)
              / nullif(count(distinct mathfit_test_id || '-' || student_id), 0),
          2)                                                                     as completion_pct,
    round(avg(case when section_status = 'COMPLETED' then section_score    end)::numeric, 2) as avg_section_score,
    round(avg(case when section_status = 'COMPLETED' then section_accuracy end)::numeric, 2) as avg_accuracy,
    sum(total_questions)                                                         as total_questions_attempted,
    sum(correct_answers)                                                         as total_correct_answers

from section_base
group by 1, 2, 3
