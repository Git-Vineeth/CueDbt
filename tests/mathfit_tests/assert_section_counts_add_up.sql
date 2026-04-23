-- Fails if section state counts don't sum to sections_assigned.
-- completed + in_progress + not_started + locked must equal sections_assigned.

select mathfit_test_id, student_id,
       sections_assigned,
       sections_completed + sections_in_progress + sections_not_started + sections_locked as sum_of_parts
from {{ ref('int_mathfit_tests__student_test_status') }}
where sections_completed + sections_in_progress + sections_not_started + sections_locked != sections_assigned
