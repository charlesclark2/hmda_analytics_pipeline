{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT 
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
        latinx.approval_rate AS lender_prior_approval_rate_latinx,
        aapi.approval_rate AS lender_prior_approval_rate_aapi,
        black.approval_rate AS lender_prior_approval_rate_black,
        white.approval_rate AS lender_prior_approval_rate_white, 
        ty_lat.tract_approval_rate as tract_prior_approval_rate_latinx, 
        ty_aapi.tract_approval_rate as tract_prior_approval_rate_aapi, 
        ty_black.tract_approval_rate as tract_prior_approval_rate_black, 
        ty_white.tract_approval_rate as tract_prior_approval_rate_white, 
        poly.income_log_x_property_value, 
        poly.gini_x_income_log,
        lm.lender_minority_gap, 
        tm.tract_minority_gap
    FROM {{ ref('mart_hmda_preprocessed') }} m 
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
    left join {{ ref('int_hmda_preprocessed_with_polynomials') }} poly 
        on m.loan_application_id = poly.loan_application_id
    left join {{ ref('int_tract_minority_gap') }} tm 
        on m.census_tract = tm.census_tract
        and m.activity_year - 1 = tm.activity_year 
    WHERE m.loan_approved IS NOT NULL
    and m.loan_application_id not in (
        select loan_application_id 
        from {{ ref('stg_validation_data') }}
    )
),

-- Join prior-year approval rates
base_with_encoding AS (
    SELECT 
        b.*,
        r.lender_prior_year_approval_rate
    FROM base b
    LEFT JOIN {{ ref('int_lender_yearly_approval_dates') }} r
      ON b.lender_entity_name = r.lender_entity_name
     AND b.activity_year - 1 = r.activity_year
),

-- Step 1: Calculate approval rates by year and race
year_race_counts AS (
    SELECT 
        activity_year,
        applicant_derived_racial_category,
        COUNT(*) AS total_count,
        SUM(CASE WHEN loan_approved = 1 THEN 1 ELSE 0 END) AS approved_count,
        SUM(CASE WHEN loan_approved = 0 THEN 1 ELSE 0 END) AS denied_count,
        CAST(SUM(CASE WHEN loan_approved = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS approval_rate
    FROM base_with_encoding
    GROUP BY activity_year, applicant_derived_racial_category
),

-- Step 2: Calculate sample size per group
sample_allocations AS (
    SELECT 
        activity_year,
        applicant_derived_racial_category,
        total_count,
        approved_count,
        denied_count,
        approval_rate,
        ROUND(7000000 * (total_count * 1.0 / (SELECT SUM(total_count) FROM year_race_counts))) AS sample_size_group
    FROM year_race_counts
),

-- Step 3: Assign random row numbers within each year, race, and approval status
stratified AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY activity_year, applicant_derived_racial_category, loan_approved
               ORDER BY RANDOM()
           ) AS rn
    FROM base_with_encoding
),

-- Step 4: Select sample rows matching approval rates within each year-race group
sampled AS (
    SELECT stratified.*
    FROM stratified
    JOIN sample_allocations sa
      ON stratified.activity_year = sa.activity_year
     AND stratified.applicant_derived_racial_category = sa.applicant_derived_racial_category
    WHERE (
        (loan_approved = 1 AND rn <= sample_size_group * approval_rate)
        OR
        (loan_approved = 0 AND rn <= sample_size_group * (1 - approval_rate))
    )
),

-- Step 5: Assign global row numbers and chunk numbers
final AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY RANDOM()) AS global_row_number,
           FLOOR((ROW_NUMBER() OVER (ORDER BY RANDOM()) - 1) / 600000) AS chunk_number
    FROM sampled
)

SELECT 
    activity_year, 
    loan_approved, 
    loan_purpose_grouped, 
    dti_bin, 
    loan_type, 
    applicant_age,  
    property_value, 
    prior_approval_rate,
    race_state_interaction, 
    income_to_loan_ratio_stratified,
    income_log, 
    applicant_derived_racial_category, 
    lender_registration_status, 
    distressed_or_underserved_race,
    minority_population_pct, 
    income, 
    loan_amount, 
    occupancy_type, 
    pct_bachelors_or_higher, 
    minority_pct_bin, 
    loan_to_income_ratio, 
    gini_index_of_income_inequality, 
    avg_median_price_per_square_foot, 
    applicant_sex_category,
    lender_prior_year_approval_rate,
    lender_prior_approval_rate_latinx,
    lender_prior_approval_rate_aapi,
    lender_prior_approval_rate_black,
    lender_prior_approval_rate_white, 
    tract_prior_approval_rate_latinx, 
    tract_prior_approval_rate_aapi, 
    tract_prior_approval_rate_black, 
    tract_prior_approval_rate_white, 
    lender_minority_gap,
    income_log_x_property_value, 
    gini_x_income_log,
    tract_minority_gap,
    global_row_number,
    chunk_number
FROM final
ORDER BY global_row_number
