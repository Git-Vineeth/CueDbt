-- Classifies a landing page URL into a category.
-- Replaces the [landingpage_grouping] Periscope macro.
-- Priority order matters — first match wins.
-- /teach/ is case-insensitively matched to Content before Intent fires.

{% macro landingpage_grouping(column_name) %}

    case
        when {{ column_name }} ilike '%job%'
            or {{ column_name }} ilike '%/teacher/%'
            or {{ column_name }} ilike '%teacher-jobs%'
            or {{ column_name }} ilike '%online-tutoring-jobs%'
            then 'Supply'

        when {{ column_name }} ilike '%worksheet%'
            or {{ column_name }} ilike '%/questions/%'
            or {{ column_name }} ilike '%ncert%'
            or {{ column_name }} ilike '%/learning/%'
            or {{ column_name }} ilike '%formula%'
            or {{ column_name }} ilike '%olympiad%'
            or {{ column_name }} ilike '%/geometry/%'
            or {{ column_name }} ilike '%/algebra/%'
            or {{ column_name }} ilike '%/calculus/%'
            or {{ column_name }} ilike '%/login/%'
            or {{ column_name }} ilike '%/events/%'
            or {{ column_name }} ilike '%/teach/%'
            then 'Content'

        when {{ column_name }} like '%/maths/class-%'
            or {{ column_name }} like '%near-me/%'
            or {{ column_name }} like '%home-tuition%'
            or {{ column_name }} like '%/home-tutors/%'
            or {{ column_name }} like '%/grades/%'
            or {{ column_name }} like '%grade-math%'
            or {{ column_name }} like '%/online-tuitions/%'
            then 'Intent'

        when {{ column_name }} ilike '%cuemath.com%'
            or {{ column_name }} ilike '%/about/%'
            or {{ column_name }} ilike '%/contact%'
            or {{ column_name }} ilike '%/pricing%'
            or {{ column_name }} ilike '%/en-gb/%'
            or {{ column_name }} ilike '%/en-ae/%'
            or {{ column_name }} ilike '%/en-us/%'
            then 'Brand'

        when {{ column_name }} ilike '%gift%'
            then 'Referral'

        else 'Others'
    end

{% endmacro %}
