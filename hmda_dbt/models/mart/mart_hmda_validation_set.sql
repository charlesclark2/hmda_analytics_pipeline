{{
    config(
        materialized='table', 
        tags=['remaining_models']
    )
}}

with base as (
    select 
        m.loan_application_id,
        m.activity_year, 
        m.loan_approved, 
        m.loan_purpose_grouped, 
        m.dti_bin, 
        m.loan_type, 
        m.applicant_age,  
        m.property_value, 
        m.prior_approval_rate,
        m.race_state_interaction, 
        m.income_to_loan_ratio_stratified,
        m.income_log, 
        m.applicant_derived_racial_category, 
        m.lender_registration_status, 
        m.distressed_or_underserved_race,
        m.minority_population_pct, 
        m.income, 
        m.loan_amount, 
        m.occupancy_type, 
        m.pct_bachelors_or_higher, 
        m.minority_pct_bin, 
        m.loan_to_income_ratio, 
        m.gini_index_of_income_inequality, 
        m.avg_median_price_per_square_foot, 
        m.applicant_sex_category,
        m.lender_entity_name, 
        r.lender_prior_year_approval_rate,
        latinx.approval_rate AS lender_prior_approval_rate_latinx,
        aapi.approval_rate AS lender_prior_approval_rate_aapi,
        black.approval_rate AS lender_prior_approval_rate_black,
        white.approval_rate AS lender_prior_approval_rate_white, 
        ty_lat.tract_approval_rate as tract_prior_approval_rate_latinx, 
        ty_aapi.tract_approval_rate as tract_prior_approval_rate_aapi, 
        ty_black.tract_approval_rate as tract_prior_approval_rate_black, 
        ty_white.tract_approval_rate as tract_prior_approval_rate_white, 
        m.income_log * m.property_value as income_log_x_property_value, 
        m.gini_index_of_income_inequality * m.income_log as gini_x_income_log,
        lm.lender_minority_gap, 
        tm.tract_minority_gap
    from {{ ref('stg_validation_data') }} m
    LEFT JOIN {{ ref('int_lender_yearly_approval_rate_by_race') }} latinx
      ON m.lender_entity_name = latinx.lender_entity_name
     AND m.activity_year - 1 = latinx.activity_year
     AND latinx.applicant_derived_racial_category = 'Latinx'

    LEFT JOIN {{ ref('int_lender_yearly_approval_rate_by_race') }} aapi
      ON m.lender_entity_name = aapi.lender_entity_name
     AND m.activity_year - 1 = aapi.activity_year
     AND aapi.applicant_derived_racial_category = 'AAPI'

    LEFT JOIN {{ ref('int_lender_yearly_approval_rate_by_race') }} black
      ON m.lender_entity_name = black.lender_entity_name
     AND m.activity_year - 1 = black.activity_year
     AND black.applicant_derived_racial_category = 'Black'

    LEFT JOIN {{ ref('int_lender_yearly_approval_rate_by_race') }} white
      ON m.lender_entity_name = white.lender_entity_name
     AND m.activity_year - 1 = white.activity_year
     AND white.applicant_derived_racial_category = 'White'
    left join {{ ref('int_tract_yearly_approvals') }} ty_lat 
        on m.census_tract = ty_lat.census_tract 
        and m.activity_year - 1 = ty_lat.activity_year 
        and ty_lat.applicant_derived_racial_category = 'Latinx'
    left join {{ ref('int_tract_yearly_approvals') }} ty_aapi 
        on m.census_tract = ty_aapi.census_tract 
        and m.activity_year - 1 = ty_aapi.activity_year 
        and ty_aapi.applicant_derived_racial_category = 'AAPI'
    left join {{ ref('int_tract_yearly_approvals') }} ty_black
        on m.census_tract = ty_black.census_tract 
        and m.activity_year - 1 = ty_black.activity_year 
        and ty_black.applicant_derived_racial_category = 'Black'
    left join {{ ref('int_tract_yearly_approvals') }} ty_white
        on m.census_tract = ty_white.census_tract 
        and m.activity_year - 1 = ty_white.activity_year 
        and ty_white.applicant_derived_racial_category = 'White'
    left join {{ ref('int_lender_minority_gap') }} lm 
        on upper(m.lender_entity_name) = upper(lm.lender_entity_name)
        and m.activity_year - 1 = lm.activity_year 
    left join {{ ref('int_tract_minority_gap') }} tm 
        on m.census_tract = tm.census_tract
        and m.activity_year - 1 = tm.activity_year 
    left join {{ ref('int_lender_yearly_approval_dates') }} r 
        on m.lender_entity_name = r.lender_entity_name
        and m.activity_year - 1 = r.activity_year
    WHERE m.loan_approved IS NOT NULL
)
select *
from base

