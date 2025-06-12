{{
    config(
        materialized='table'
    )
}}

with base as (
    select 
        p.activity_year,
        s.applicant_derived_racial_category as race, 
        s.predicted_label,
        s.actual_value
    from {{ source('other', 'scored_results') }} s 
    join {{ ref('mart_hmda_preprocessed') }} p 
    on s.loan_application_id = p.loan_application_id
    where race not like 'Unknown'
), 
grouped as (
    select 
        activity_year, 
        race, 
        count(*) as total_apps, 
        sum(case when predicted_label = 1 then 1 else 0 end) as total_predicted_approvals,
        sum(case when actual_value = 1 then 1 else 0 end) as total_actual_approvals
    from base 
    group by activity_year, race 
)
select 
    activity_year, 
    race, 
    total_predicted_approvals / nullif(total_apps, 0) as predicted_approval_rate, 
    total_actual_approvals / nullif(total_apps, 0) as actual_approval_rate 
from grouped 