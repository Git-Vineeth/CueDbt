with source as (
    select * from {{ source('concepts', 'mathfit_test') }}
),

renamed as (
    select
        id              as mathfit_test_id,
        user_id         as student_id,
        score           as overall_score,   -- null until all sections are COMPLETED
        created_on,
        updated_on,
        meta_data
    from source
)

select * from renamed
