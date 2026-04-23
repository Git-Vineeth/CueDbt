{% macro safe_divide(numerator, denominator, decimal_places=2) %}
    round(
        100.0 * {{ numerator }} / nullif({{ denominator }}, 0),
        {{ decimal_places }}
    )
{% endmacro %}
