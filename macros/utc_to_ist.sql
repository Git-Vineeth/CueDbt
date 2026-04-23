{% macro utc_to_ist(column_name) %}
    {{ column_name }} + interval '330 minutes'
{% endmacro %}
