{{
    config(
        materialized='table'
    )
}}

with raw as (
    select * from {{ ref('int_hmda_all_years') }}
), 
filtered as (
    select *
    from raw 
    where action_taken in (1, 2, 3, 7, 8)
    and loan_amount is not null 
    and income is not null 
)

select * 
from filtered
