-- Redshift uses two-part naming (schema.table), not three-part (database.schema.table).
-- Without this, Elementary's default__target_database() returns target.dbname ("cuemath"),
-- causing "X does not exist" errors when Elementary models build relation references.
{% macro redshift__target_database() %}
    {% do return(none) %}
{% endmacro %}

-- Redshift rejects BEGIN...COMMIT as a single prepared statement.
-- This override splits the delete and insert into separate run_query calls,
-- matching the pattern already used by Spark/Athena/Trino adapters in Elementary.
{% macro redshift__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set queries = [] %}

    {% if delete_relation %}
        {% set delete_query %}
            delete from {{ relation }}
            where {{ delete_column_key }} is null
               or {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
        {% endset %}
        {% do queries.append(delete_query) %}
    {% endif %}

    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}

    {% do return(queries) %}
{% endmacro %}
