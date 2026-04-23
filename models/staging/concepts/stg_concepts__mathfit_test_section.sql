with source as (
    select * from {{ source('concepts', 'mathfit_test_section') }}
),

renamed as (
    select
        id                                      as section_id,
        mathfit_test_id,
        user_node_id,
        section_number,
        score                                   as section_score,  -- null until section is COMPLETED
        created_on,
        created_on::date                        as created_on_date,
        to_char(created_on, 'YYYY-MM')          as created_on_month,
        updated_on,
        meta_data
    from source
)

select * from renamed
