-- state column is the source of truth for completion status — not timestamps.
-- Dropped columns: ceremony_completed_on (100% null), reattempt_unlocked_on (100% null),
--                  due_date (99.5% null).

with source as (
    select * from {{ source('concepts', 'user_node') }}
),

renamed as (
    select
        id                          as user_node_id,
        user_id                     as student_id,
        node_id,
        worksheet_id,
        program_id,
        user_chapter_id,
        user_block_id,
        course_type,
        state,
        is_deleted,
        is_live,
        is_timed,
        is_attempt_in_progress,
        complete_status,
        reattempt_status,
        unlock_status,
        attempt_location,
        last_attempt_id,
        last_attempt_state,
        accuracy,
        time_spent,
        points_earned,
        started_on,
        completed_on,
        last_attempted_on,
        unlocked_on,
        created_on,
        updated_on,
        meta_data,
        last_attempt_info
    from source
)

select * from renamed
