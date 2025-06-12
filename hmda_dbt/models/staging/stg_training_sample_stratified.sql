{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        activity_year,
        loan_approved,
        loan_purpose_grouped,
        lender_entity_name,
        loan_purpose,
        dti_bin,
        debt_to_income_ratio,
        loan_amount_threshold,
        loan_type,
        lender_entity_age,
        property_value_log,
        property_value_capped_and_log,
        property_value_capped,
        applicant_age,
        property_value,
        prior_approval_rate,
        race_state_interaction,
        income_to_loan_ratio_stratified,
        income_log,
        has_property_value,
        income_to_loan_ratio_stratified_capped,
        applicant_derived_racial_category,
        lender_registration_status,
        income_to_loan_ratio_stratified_capped_and_log,
        distressed_or_underserved_race,
        minority_population_pct,
        income,
        loan_amount,
        occupancy_type,
        loan_amount_capped_and_log,
        race_sex_interaction,
        pct_bachelors_or_higher,
        income_capped,
        income_capped_and_log,
        loan_amount_log,
        loan_amount_capped,
        minority_pct_bin,
        loan_to_income_ratio,
        loan_to_income_ratio_capped,
        gini_index_of_income_inequality,
        avg_median_price_per_square_foot,
        applicant_sex_category
    FROM {{ ref('mart_hmda_preprocessed') }}
    WHERE loan_approved IS NOT NULL
),

lender_target_encoding AS (
    SELECT 
        lender_entity_name,
        mean(loan_approved) AS approval_rate
    FROM {{ ref('mart_hmda_preprocessed') }}
    GROUP BY lender_entity_name
),

global_approval_rate AS (
    SELECT mean(loan_approved) AS global_rate FROM {{ ref('mart_hmda_preprocessed') }}
),

base_with_te AS (
    SELECT 
        b.*,
        COALESCE(lte.approval_rate, gar.global_rate) AS lender_entity_name_te
    FROM base b
    LEFT JOIN lender_target_encoding lte
      ON b.lender_entity_name = lte.lender_entity_name
    CROSS JOIN global_approval_rate gar
),

-- Assign row numbers separately for each class
class_rows AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY loan_approved ORDER BY RANDOM()) AS rn_within_class
    FROM base_with_te
),

-- Add total counts for each class
class_counts AS (
    SELECT 
        loan_approved, 
        COUNT(*) AS class_count
    FROM class_rows
    GROUP BY loan_approved
),

-- Create interleaved global row numbers
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

-- Assign chunks based on the new interleaved ordering
final AS (
    SELECT 
        *,
        FLOOR((global_row_number - 1) / 400000) AS chunk_number
    FROM interleaved
)

SELECT 
    activity_year,
    loan_approved,
    loan_purpose_grouped,
    lender_entity_name,
    lender_entity_name_te,
    loan_purpose,
    dti_bin,
    debt_to_income_ratio,
    loan_amount_threshold,
    loan_type,
    lender_entity_age,
    property_value_log,
    property_value_capped_and_log,
    property_value_capped,
    applicant_age,
    property_value,
    prior_approval_rate,
    race_state_interaction,
    income_to_loan_ratio_stratified,
    income_log,
    has_property_value,
    income_to_loan_ratio_stratified_capped,
    applicant_derived_racial_category,
    lender_registration_status,
    income_to_loan_ratio_stratified_capped_and_log,
    distressed_or_underserved_race,
    minority_population_pct,
    income,
    loan_amount,
    occupancy_type,
    loan_amount_capped_and_log,
    race_sex_interaction,
    pct_bachelors_or_higher,
    income_capped,
    income_capped_and_log,
    loan_amount_log,
    loan_amount_capped,
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
