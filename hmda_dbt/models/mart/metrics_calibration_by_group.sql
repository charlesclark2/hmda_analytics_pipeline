{{
    config(
        materialized='table'
    )
}}

with base as (
    select 
        applicant_derived_racial_category, 
        score, 
        actual_value 
    from {{ source('other', 'scored_results') }}
), 
binned as (
    select 
        applicant_derived_racial_category, 
        width_bucket(score, 0.0, 1.0, 5) as score_bin, 
        count(*) as n,
        avg(score) as avg_predicted_score, 
        avg(actual_value) as actual_approval_rate
    from base 
    group by applicant_derived_racial_category, score_bin 
)
select *
from binned 