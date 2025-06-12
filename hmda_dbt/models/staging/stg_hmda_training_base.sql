{{ config(
    materialized='table'
) }}

select
    *
from {{ ref('mart_hmda_preprocessed') }}
where loan_approved is not null
order by activity_year, loan_purpose_grouped, loan_approved
