{{
    config(
        materialized='table'
    )
}}

with base as (
    select 
        activity_year,
        loan_approved,
        loan_purpose_grouped,
        loan_purpose,
        dti_bin,
        loan_type,
        applicant_age,
        property_value,
        prior_approval_rate,
        race_state_interaction,
        income_to_loan_ratio_stratified,
        income_log,
        has_property_value,
        applicant_derived_racial_category,
        lender_registration_status,
        distressed_or_underserved_race,
        minority_population_pct,
        income,
        loan_amount,
        occupancy_type,
        race_sex_interaction,
        pct_bachelors_or_higher,
        minority_pct_bin,
        loan_to_income_ratio,
        loan_to_income_ratio_capped,
        gini_index_of_income_inequality,
        avg_median_price_per_square_foot,
        applicant_sex_category
    from {{ ref('mart_hmda_preprocessed') }}
    where activity_year in (2018, 2019, 2020, 2021, 2022)
),
approval_cnts as (
    select 
        activity_year, 
        count_if(loan_approved = 1) approval_cnt, 
        count_if(loan_approved = 0) denial_cnt, 
        approval_cnt + denial_cnt as total_cnt,
        round((approval_cnt / (approval_cnt + denial_cnt)) * 100, 2) as approval_rate, 
        count_if(applicant_derived_racial_category like 'AAPI') aapi_cnt, 
        round((aapi_cnt / total_cnt) * 100, 4) as aapi_pct,
        count_if(applicant_derived_racial_category like 'AAPI' and loan_approved = 1) aapi_approval_cnt,
        count_if(applicant_derived_racial_category like 'AAPI' and loan_approved = 0) aapi_denial_cnt,
        round((aapi_approval_cnt / aapi_cnt) * 100, 4) as aapi_approval_rate,
        count_if(applicant_derived_racial_category like 'Latinx') latinx_cnt, 
        round((latinx_cnt / total_cnt) * 100, 4) as latinx_pct,
        count_if(applicant_derived_racial_category like 'Latinx' and loan_approved = 1) latinx_approval_cnt,
        count_if(applicant_derived_racial_category like 'Latinx' and loan_approved = 0) latinx_denial_cnt,
        round((latinx_approval_cnt / latinx_cnt) * 100, 4) as latinx_approval_rate,
        count_if(applicant_derived_racial_category like 'Black') black_cnt, 
        round((black_cnt / total_cnt) * 100, 4) as black_pct,
        count_if(applicant_derived_racial_category like 'Black' and loan_approved = 1) black_approval_cnt,
        count_if(applicant_derived_racial_category like 'Black' and loan_approved = 0) black_denial_cnt,
        round((black_approval_cnt / black_cnt) * 100, 4) as black_approval_rate,
        count_if(applicant_derived_racial_category like 'White') white_cnt, 
        round((white_cnt / total_cnt) * 100, 4) as white_pct,
        count_if(applicant_derived_racial_category like 'White' and loan_approved = 1) white_approval_cnt,
        count_if(applicant_derived_racial_category like 'White' and loan_approved = 0) white_denial_cnt,
        round((white_approval_cnt / white_cnt) * 100, 4) as white_approval_rate,
        count_if(applicant_derived_racial_category like 'Unknown') unknown_cnt, 
        round((unknown_cnt / total_cnt) * 100, 4) as unknown_pct,
        count_if(applicant_derived_racial_category like 'Unknown' and loan_approved = 1) unknown_approval_cnt,
        count_if(applicant_derived_racial_category like 'Unknown' and loan_approved = 0) unknown_denial_cnt,
        round((unknown_approval_cnt / unknown_cnt) * 100, 4) as unknown_approval_rate
    from base 
    group by activity_year
)
select 
    activity_year, 
    approval_rate, 
    aapi_pct, 
    aapi_approval_rate, 
    latinx_pct, 
    latinx_approval_rate, 
    black_pct, 
    black_approval_rate, 
    white_pct, 
    white_approval_rate, 
    unknown_pct, 
    unknown_approval_rate
from approval_cnts 
order by activity_year asc