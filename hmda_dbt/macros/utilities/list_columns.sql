{% macro get_column_list_from_source(source_name, table_name) %}
  {% set relation = source(source_name, table_name) %}
  {% set columns = adapter.get_columns_in_relation(relation) %}
  {{ return(columns | map(attribute='name') | join(', ')) }}
{% endmacro %}
