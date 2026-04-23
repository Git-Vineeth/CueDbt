-- Fails if a test where sections_completed = sections_assigned is NOT labeled FULL_COMPLETION.

select mathfit_test_id, student_id, sections_completed, sections_assigned, test_status
from {{ ref('int_mathfit_tests__student_test_status') }}
where sections_completed = sections_assigned
  and sections_assigned > 0
  and test_status != 'FULL_COMPLETION'
