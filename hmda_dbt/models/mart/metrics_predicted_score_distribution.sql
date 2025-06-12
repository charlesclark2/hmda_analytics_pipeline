{{ 
    config(
        materialized='table'
    )
}}

with base as (
    select  
        p.activity_year, 
        s.score, 
        s.actual_value, 
        s.applicant_derived_racial_category
    from {{ source('other', 'scored_results') }} s 
    join (
        select loan_application_id, activity_year 
        from {{ ref('mart_hmda_preprocessed') }}
    ) p 
    on s.loan_application_id = p.loan_application_id
    where s.applicant_derived_racial_category not like 'Unknown'
), 
binned as (
    select 
        applicant_derived_racial_category, 
        activity_year, 
        actual_value, 
        width_bucket(score, 0.0, 1.0, 10) as score_bin, 
        count(*) as n_in_bin, 
        avg(score) as avg_score 
    from base 
    group by applicant_derived_racial_category, activity_year, actual_value, 
    width_bucket(score, 0.0, 1.0, 10)
)
select *
from binned
order by score_bin asc, activity_year asc 
