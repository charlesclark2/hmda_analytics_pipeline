-- macros/tests/not_null_all_columns.sql

{% test not_null_all_columns(model) %}
    {% set columns = adapter.get_columns_in_relation(model) %}

    select *
    from {{ model }}
    where
        {% for column in columns %}
            {{ column.name }} is null{% if not loop.last %} or{% endif %}
        {% endfor %}
{% endtest %}
