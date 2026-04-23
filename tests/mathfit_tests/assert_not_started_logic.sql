-- Fails if a test with no completed and no in-progress sections is NOT labeled NOT_STARTED.

select mathfit_test_id, student_id, sections_completed, sections_in_progress, test_status
from {{ ref('int_mathfit_tests__student_test_status') }}
where sections_completed = 0
  and sections_in_progress = 0
  and test_status != 'NOT_STARTED'
