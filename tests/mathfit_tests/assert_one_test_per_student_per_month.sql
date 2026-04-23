-- Fails if a student has more than one test in the same month.
-- Duplicate (student_id, month) pairs cause double-counting in all mart models.

select student_id, created_on_month, count(*) as test_count
from {{ ref('int_mathfit_tests__student_test_status') }}
group by 1, 2
having count(*) > 1
