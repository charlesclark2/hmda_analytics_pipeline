{{
    config(
        materialized='table', 
        tag=["remaining_models"]
    )
}}

with base as (
    select *
    from {{ ref('int_hmda_enriched_with_distress') }}
), 
cat_xref as (
    select *
    from {{ source('other', 'hmda_categories_xref') }}
),
lei as (
    select *
    from {{ ref('int_legal_entity_identifier_xref') }}
),
features as (
select 
    -- mortgage metadata
    b.loan_application_id, 
    b.activity_year,
    coalesce(b.zip_code, 'Unknown') as zip_code, 
    b.state_code, 
    b.county_code, 
    b.census_tract,
    case 
        when b.action_taken in (1, 2) then 1 
        else 0 
    end as loan_approved,
    -- lender information
    coalesce(l.entity_name, 'UNKNOWN') as lender_entity_name, 
    coalesce(l.registration_status, 'UNKNOWN') as lender_registration_status, 
    coalesce(l.entity_age, 0) as lender_entity_age,  
    -- applicant demographics
    c2.category_title as applicant_race_category,
    c1.category_title as applicant_ethnicity_category,
    case 
        when c1.category_title is null 
            and c2.category_title not in ('not provided', 'not applicable')
            and c2.category_title is not null 
                then c2.category_title
        when c1.category_title like 'not applicable' 
            and c2.category_title like 'not provided'
            then null 
        when c1.category_title like 'not applicable'
            and c2.category_title not in ('not applicable', 'not provided')
            and c2.category_title is not null 
            then c2.category_title
        when c1.category_title like 'information not provided'
            and c2.category_title not in ('not applicable', 'not provided')
            and c2.category_title is not null 
            then c2.category_title
        when c1.category_title like 'Not hisp%'
            and c2.category_title not in ('not applicable', 'not provided')
            and c2.category_title is not null 
            then c2.category_title
        when c1.category_title like 'no co%'
            and c2.category_title not in ('not applicable', 'not provided')
            and c2.category_title is not null 
            then c2.category_title
        when (c1.category_title in ('not applicable', 'information not provided', 'Not hispanic or latinx')
                or c1.category_title is null)
            and (c2.category_title in ('not applicable', 'not provided') or c2.category_title is null)
            then null 
        when c1.category_title = 'Latinx' then 'Latinx'
        else 'not a match'
    end as applicant_derived_racial_category,
    c3.category_title as applicant_sex_category,
    b.applicant_age, 
    b.income, 
    b.debt_to_income_ratio, 

    -- loan information
    b.loan_amount, 
    b.loan_type, 
    b.loan_purpose, 
    b.occupancy_type, 
    b.property_value, 

    -- tract level features
    b.median_household_income, 
    b.minority_population_pct, 
    b.pct_bachelors_or_higher, 
    b.total_population_below_poverty_level_pct, 
    b.gini_index_of_income_inequality, 
    b.income_bucketed, 
    b.is_distressed, 
    b.is_underserved, 

    -- zillow features
    b.avg_median_sale_price, 
    b.avg_median_sale_price_year_over_year, 
    b.avg_median_list_price, 
    b.avg_median_list_price_year_over_year, 
    b.avg_median_price_per_square_foot, 
    b.avg_median_price_per_square_foot_year_over_year, 
    b.avg_inventory, 
    b.avg_new_listings, 
    b.avg_median_days_on_market

from base b 
left join cat_xref c1 
    on try_cast(b.applicant_ethnicity as integer) = c1.category_value
    and c1.column_name = 'applicant_ethnicity'
left join cat_xref c2 
    on try_cast(b.applicant_race as integer) = c2.category_value
    and c2.column_name = 'applicant_race'
left join cat_xref c3 
    on try_cast(b.applicant_sex as integer) = c3.category_value 
    and c3.column_name = 'applicant_sex'
left join lei l 
    on upper(b.legal_entity_identifier) = upper(l.lei)
where loan_amount is not null 
and income is not null 
and applicant_race is not null 
and action_taken in (1, 2, 3)
), 
state_code_cleaning as (
select 
    b.loan_application_id, 
    b.activity_year,
    b.zip_code, 
    b.state_code, 
    b.census_tract,
    substring(b.census_tract, 1, 2) as fips_state_code, 
    coalesce(b.county_code, substring(b.census_tract, 3, 3)) as county_code_modified,
    coalesce(b.state_code, st.usps) as state_code_modified,
    b.loan_approved,
    -- lender information
    b.lender_entity_name, 
    b.lender_registration_status, 
    b.lender_entity_age,  
    -- applicant demographics
    b.applicant_race_category,
    b.applicant_ethnicity_category,
    b.applicant_derived_racial_category,
    b.applicant_sex_category,
    b.applicant_age, 
    b.income, 
    b.debt_to_income_ratio, 

    -- loan information
    b.loan_amount, 
    b.loan_type, 
    b.loan_purpose, 
    b.occupancy_type, 
    b.property_value, 

    -- tract level features
    b.median_household_income, 
    b.minority_population_pct, 
    b.pct_bachelors_or_higher, 
    b.total_population_below_poverty_level_pct, 
    b.gini_index_of_income_inequality, 
    b.income_bucketed, 
    b.is_distressed, 
    b.is_underserved, 

    -- zillow features
    b.avg_median_sale_price, 
    b.avg_median_sale_price_year_over_year, 
    b.avg_median_list_price, 
    b.avg_median_list_price_year_over_year, 
    b.avg_median_price_per_square_foot, 
    b.avg_median_price_per_square_foot_year_over_year, 
    b.avg_inventory, 
    b.avg_new_listings, 
    b.avg_median_days_on_market
from features b 
left join {{ source('other', 'state_fips_xref') }} st 
    on substring(lpad(b.census_tract, 11, '0'), 1, 2) = st.fips
), 
final as (
    select 
        loan_application_id, 
        activity_year,
        zip_code, 
        state_code_modified as state_code, 
        county_code_modified as county_code,
        census_tract,
        loan_approved,
        -- lender information
        lender_entity_name, 
        lender_registration_status, 
        lender_entity_age,  
        -- applicant demographics
        applicant_race_category,
        applicant_ethnicity_category,
        applicant_derived_racial_category,
        applicant_sex_category,
        applicant_age, 
        income, 
        debt_to_income_ratio, 

        -- loan information
        loan_amount, 
        loan_type, 
        loan_purpose, 
        occupancy_type, 
        property_value, 

        -- tract level features
        median_household_income, 
        minority_population_pct, 
        pct_bachelors_or_higher, 
        total_population_below_poverty_level_pct, 
        gini_index_of_income_inequality, 
        income_bucketed, 
        is_distressed, 
        is_underserved, 

        -- zillow features
        avg_median_sale_price, 
        avg_median_sale_price_year_over_year, 
        avg_median_list_price, 
        avg_median_list_price_year_over_year, 
        avg_median_price_per_square_foot, 
        avg_median_price_per_square_foot_year_over_year, 
        avg_inventory, 
        avg_new_listings, 
        avg_median_days_on_market
    from state_code_cleaning
)
select *
from final
where state_code is not null
