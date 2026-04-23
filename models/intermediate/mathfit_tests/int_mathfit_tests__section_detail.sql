-- Grain: one row per section per student per test.
-- Pure joins only — no aggregation here.
-- section_status is derived from user_node.state (source of truth, not timestamps).

with base as (

    select
        mfs.section_id,
        mfs.mathfit_test_id,
        mfs.user_node_id,
        mfs.section_number,
        mfs.section_score,
        mfs.created_on,
        mfs.created_on_date,
        mfs.created_on_month,

        mft.student_id,
        mft.overall_score,

        un.complete_status,
        un.is_attempt_in_progress,
        un.is_deleted,
        un.accuracy                         as section_accuracy,
        un.time_spent                       as section_time_spent,
        un.last_attempt_id,
        un.course_type,
        un.started_on,
        un.completed_on,
        un.unlocked_on                      as section_unlocked_on,
        un.meta_data.title::text            as section_title,
        un.meta_data.correct_answers        as correct_answers_meta,

        ua.total_questions,
        ua.correct_answers,
        ua.accuracy                         as attempt_accuracy,
        ua.time_spent                       as attempt_time_spent,

        case
            when un.state = 'COMPLETED'     then 'COMPLETED'
            when un.state = 'IN_PROGRESS'   then 'IN_PROGRESS'
            when un.state = 'NOT_STARTED'   then 'NOT_STARTED'
            when un.state = 'LOCKED'        then 'LOCKED'
            else un.state
        end                                 as section_status

    from {{ ref('stg_concepts__mathfit_test_section') }} mfs
    left join {{ ref('stg_concepts__mathfit_test') }} mft
        on mfs.mathfit_test_id = mft.mathfit_test_id
    left join {{ ref('stg_concepts__user_node') }} un
        on mfs.user_node_id = un.user_node_id
    left join {{ ref('stg_concepts__user_attempt') }} ua
        on un.last_attempt_id = ua.attempt_id

)

select * from base
where mathfit_test_id != '879094ce-d97c-11f0-8895-96675ccd88a1'
-- Known data anomaly: state=NOT_STARTED but complete_status=True with section_score populated.
-- Confirmed 2026-04-04. student_id: cd6cd488-4016-4434-a917-a5378aef9942.
-- Flagged to dev team. Root cause unknown.
