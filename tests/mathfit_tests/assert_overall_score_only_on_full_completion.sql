-- Fails if overall_score is populated for a test that is not FULL_COMPLETION.
-- overall_score should only exist when all sections are COMPLETED.

select mathfit_test_id, student_id, test_status, overall_score
from {{ ref('int_mathfit_tests__student_test_status') }}
where overall_score is not null
  and test_status != 'FULL_COMPLETION'
