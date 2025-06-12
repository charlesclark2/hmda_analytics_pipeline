{{ config(materialized='table') }}

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
      AND activity_year IN (2018, 2019, 2020, 2021, 2022, 2023)
),

-- Stratified interleaving logic for balanced chunks
class_rows AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY loan_approved ORDER BY RANDOM()) AS rn_within_class
    FROM base
),

class_counts AS (
    SELECT 
        loan_approved, 
        COUNT(*) AS class_count
    FROM class_rows
    GROUP BY loan_approved
),

interleaved AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY 
                COALESCE(rn_within_class * (CASE WHEN loan_approved = 1 THEN (SELECT MIN(class_count) FROM class_counts) ELSE 1 END), 0) +
                COALESCE(rn_within_class * (CASE WHEN loan_approved = 0 THEN (SELECT MIN(class_count) FROM class_counts) ELSE 1 END), 0)
        ) AS global_row_number
    FROM class_rows
),

final AS (
    SELECT 
        *,
        FLOOR((global_row_number - 1) / 600000) AS chunk_number
    FROM interleaved
    WHERE global_row_number <= 15000000  -- Limit to ~15M rows
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
