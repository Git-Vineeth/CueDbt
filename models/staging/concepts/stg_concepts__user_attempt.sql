-- Dropped columns: points_earned (100% null), canvas_url (100% null).
-- total_questions, correct_answers, accuracy, time_spent are null until attempt is completed.

with source as (
    select * from {{ source('concepts', 'user_attempt') }}
),

renamed as (
    select
        id                      as attempt_id,
        user_id                 as student_id,
        user_node_id,
        node_id,
        worksheet_id,
        program_id,
        course_type,
        state,
        attempt_location,
        is_timed,
        learnosity_activity_ref,
        total_questions,
        correct_answers,
        accuracy,
        total_mistake,
        perfect_hit,
        skip_counter,
        time_spent,
        doubts_resolved_count,
        started_on,
        completed_on,
        last_attempted_on,
        canvas_expiry_date,
        created_on,
        updated_on,
        meta_data,
        attempt_info
    from source
)

select * from renamed
