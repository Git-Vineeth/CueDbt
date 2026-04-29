-- Redshift rejects three-part naming (database.schema.table) in DML statements.
-- Without this override, Elementary's default__target_database() returns target.dbname
-- ("cuemath"), causing "X does not exist" errors in relation references.
{% macro redshift__target_database() %}
    {% do return(none) %}
{% endmacro %}

-- Redshift override for get_package_database_and_schema.
-- The default reads node.database from the graph (= "cuemath") and returns it.
-- For Redshift DML, the database component must be none to use two-part naming.
{% macro redshift__get_package_database_and_schema(package_name="elementary") %}
    {% if execute %}
        {% set node_in_package = (
            graph.nodes.values()
            | selectattr("resource_type", "==", "model")
            | selectattr("package_name", "==", package_name)
            | first
        ) %}
        {% if node_in_package %}
            {{ return([none, node_in_package.schema]) }}
        {% endif %}
    {% endif %}
    {{ return([none, none]) }}
{% endmacro %}

-- Redshift override for get_elementary_relation.
-- The default reads identifier_node.database directly from the graph ("cuemath") and
-- passes it to adapter.get_relation(), which serialises as "cuemath"."schema"."table".
-- Redshift rejects this three-part form in INSERT INTO / DML, causing the on-run-end
-- hook to fail silently and leave elementary_test_results / dbt_run_results empty.
-- Passing none drops the database prefix so all DML uses schema.table (two-part).
{% macro get_elementary_relation(identifier) %}
    {%- if execute %}
        {%- set identifier_node = elementary.get_node("model.elementary." ~ identifier) %}
        {%- if identifier_node -%}
            {%- set identifier_alias = elementary.safe_get_with_default(
                identifier_node, "alias", identifier
            ) %}
            {%- set elementary_schema = identifier_node.schema %}
        {%- else -%}
            {%- set identifier_alias = identifier %}
            {%- set db_schema = elementary.get_package_database_and_schema() %}
            {%- set elementary_schema = db_schema[1] %}
        {%- endif -%}
        {%- if this and this.schema == elementary_schema and this.identifier == identifier_alias %}
            {% do return(this) %}
        {%- endif %}
        {% do return(
            adapter.get_relation(none, elementary_schema, identifier_alias)
        ) %}
    {%- endif %}
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
