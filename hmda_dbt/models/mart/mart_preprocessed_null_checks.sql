{{ 
    config(
        materialized='table'
    )
}}

{% set columns = adapter.get_columns_in_relation(ref('mart_hmda_preprocessed')) %}
with row_counts as (
    select 
        count(*) as total_rows
    from {{ ref('mart_hmda_preprocessed') }}
),
null_counts as (
    select 
        {% for column in columns %}
            sum(case when {{ column.name }} is null then 1 else 0 end) as {{ column.name }}_null_count{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('mart_hmda_preprocessed') }}
)
select *
from row_counts
cross join null_counts
