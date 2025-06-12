{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT 
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
        applicant_sex_category,
        ROW_NUMBER() OVER () AS internal_row_id
    FROM {{ ref('mart_hmda_preprocessed') }}
    WHERE loan_approved IS NOT NULL
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
    FROM base
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
        -- Adjust 15000000 for your desired total sample size
        ROUND(4000000* (total_count * 1.0 / (SELECT SUM(total_count) FROM year_race_counts))) AS sample_size_group
    FROM year_race_counts
),

-- Step 3: Assign random row numbers within each year, race, and approval status
stratified AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY activity_year, applicant_derived_racial_category, loan_approved
               ORDER BY RANDOM()
           ) AS rn
    FROM base
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

-- Step 5: Assign global row numbers and chunk numbers for Dask/distributed processing
final AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY RANDOM()) AS global_row_number,
           FLOOR((ROW_NUMBER() OVER (ORDER BY RANDOM()) - 1) / 400000) AS chunk_number
    FROM sampled
)

SELECT 
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
    applicant_sex_category,
    global_row_number,
    chunk_number
FROM final
ORDER BY global_row_number
