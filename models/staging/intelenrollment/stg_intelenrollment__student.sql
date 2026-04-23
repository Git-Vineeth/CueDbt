-- Grain: one row per student.
-- Provides student-level region derived from country_code.
-- Region: country_code 1 = NAM (North America), 91 = ISC (India), all others = ROW.

with source as (
    select * from {{ source('intelenrollment', 'student') }}
)

select
    id                                                  as student_id,
    country_code,
    case
        when country_code = 1  then 'NAM'
        when country_code = 91 then 'ISC'
        else                        'ROW'
    end                                                 as region
from source
