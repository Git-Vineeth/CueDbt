-- Fails if section_score is populated for a section that is not COMPLETED.
-- section_score should only exist when the section state is COMPLETED.

select section_id, student_id, section_status, section_score
from {{ ref('int_mathfit_tests__section_detail') }}
where section_score is not null
  and section_status != 'COMPLETED'
