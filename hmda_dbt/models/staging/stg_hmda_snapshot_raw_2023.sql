{{ 
    config(
        materialized='table'
    ) 
}}

with source as (
    select * from {{ source('hmda', 'hmda_snapshot_raw_2023') }}
),

renamed as (
    select 
        md5_hex(
            coalesce(activity_year, '') || '-' ||
            coalesce(action_taken, '') || '-' ||
            coalesce(lei, '') || '-' ||
            coalesce(state_code, '') || '-' ||
            coalesce(county_code, '') || '-' ||
            coalesce(census_tract, '') || '-' ||
            coalesce(loan_amount, '') || '-' ||
            coalesce(income, '') || '-' ||
            coalesce(applicant_ethnicity_1, '') || '-' ||
            coalesce(applicant_race_1, '') || '-' ||
            coalesce(applicant_sex, '')
        ) as loan_application_id,

        -- loan metadata
        try_cast(activity_year as integer) as activity_year,
        lei as legal_entity_identifier,
        try_cast(action_taken as integer) as action_taken,

        -- geography
        try_cast(derived_msa_md as integer) as derived_msa_md,
        state_code,
        county_code,
        lpad(census_tract, 11, '0') as census_tract,

        -- loan information
        try_cast(loan_amount as integer) as loan_amount,
        try_cast(loan_type as integer) as loan_type,
        try_cast(loan_purpose as integer) as loan_purpose,
        try_cast(lien_status as integer) as lien_status,
        try_cast(construction_method as integer) as construction_method,
        try_cast(property_value as integer) as property_value,
        try_cast(occupancy_type as integer) as occupancy_type,

        -- applicant demographics
        try_cast(applicant_race_1 as integer) as applicant_race,
        try_cast(applicant_ethnicity_1 as integer) as applicant_ethnicity,
        try_cast(applicant_sex as integer) as applicant_sex,
        case 
            when applicant_age in ('8888', '9999') then null
            else applicant_age
        end as applicant_age,
        case 
            when try_cast(income as bigint) > 0 then try_cast(income as bigint) * 100
            when try_cast(income as bigint) < 0 then try_cast(income as bigint) * 100
            else null
        end as income,
        case 
            when debt_to_income_ratio ilike 'Exempt' then null
            else debt_to_income_ratio
        end as debt_to_income_ratio,

        -- pricing/cost
        case when interest_rate ilike 'Exempt' then null else try_cast(interest_rate as float) end as interest_rate,
        case when rate_spread ilike 'Exempt' then null else try_cast(rate_spread as float) end as rate_spread,
        case when origination_charges ilike 'Exempt' then null else try_cast(origination_charges as decimal) end as origination_charges,
        case when lender_credits ilike 'Exempt' then null else try_cast(lender_credits as decimal) end as lender_credits,
        case when discount_points ilike 'Exempt' then null else try_cast(discount_points as decimal) end as discount_points,
        case when total_loan_costs ilike 'Exempt' then null else try_cast(total_loan_costs as decimal) end as total_loan_costs,
        case when total_points_and_fees ilike 'Exempt' then null else try_cast(total_points_and_fees as decimal) end as total_points_and_fees,

        -- risk flags
        case when negative_amortization = '1111' then null else try_cast(negative_amortization as integer) end as negative_amortization,
        case when interest_only_payment = '1111' then null else try_cast(interest_only_payment as integer) end as interest_only_payment,
        case when balloon_payment = '1111' then null else try_cast(balloon_payment as integer) end as balloon_payment,
        case when other_nonamortizing_features = '1111' then null else try_cast(other_nonamortizing_features as integer) end as other_nonamortizing_features,
        case when prepayment_penalty_term ilike 'Exempt' then null else try_cast(prepayment_penalty_term as integer) end as prepayment_penalty_term,

        -- AUS
        case when aus_1 = '1111' then null else try_cast(aus_1 as integer) end as aus_1,
        case when aus_2 = '1111' then null else try_cast(aus_2 as integer) end as aus_2,
        case when aus_3 = '1111' then null else try_cast(aus_3 as integer) end as aus_3,
        case when aus_4 = '1111' then null else try_cast(aus_4 as integer) end as aus_4,

        -- co-applicant
        try_cast(co_applicant_race_1 as integer) as co_applicant_race,
        try_cast(co_applicant_ethnicity_1 as integer) as co_applicant_ethnicity,
        try_cast(co_applicant_sex as integer) as co_applicant_sex,
        case 
            when co_applicant_age in ('8888', '9999') then null
            else co_applicant_age
        end as co_applicant_age,

        -- application process
        case when submission_of_application = '1111' then null else try_cast(submission_of_application as integer) end as submission_of_application,
        case when initially_payable_to_institution = '1111' then null else try_cast(initially_payable_to_institution as integer) end as initially_payable_to_institution,
        try_cast(preapproval as integer) as preapproval,

        row_number() over (partition by 
            md5_hex(
                coalesce(activity_year, '') || '-' ||
                coalesce(action_taken, '') || '-' ||
                coalesce(lei, '') || '-' ||
                coalesce(state_code, '') || '-' ||
                coalesce(county_code, '') || '-' ||
                coalesce(census_tract, '') || '-' ||
                coalesce(loan_amount, '') || '-' ||
                coalesce(income, '') || '-' ||
                coalesce(applicant_ethnicity_1, '') || '-' ||
                coalesce(applicant_race_1, '') || '-' ||
                coalesce(applicant_sex, '')
            )
            order by activity_year
        ) as rn

    from source
)

select *
from renamed
where rn = 1
