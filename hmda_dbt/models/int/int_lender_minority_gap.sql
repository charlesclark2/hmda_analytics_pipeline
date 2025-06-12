{{ 
    config(
        materialized='table', 
        tags=["remaining_models"]
    ) 
}}

WITH lender_year_race AS (
    SELECT 
        lender_entity_name,
        activity_year,
        applicant_derived_racial_category,
        COUNT(*) AS total_apps,
        SUM(CASE WHEN loan_approved = 1 THEN 1 ELSE 0 END) AS approved_apps
    FROM {{ ref('mart_hmda_preprocessed') }}
    WHERE loan_approved IS NOT NULL
    GROUP BY lender_entity_name, activity_year, applicant_derived_racial_category
),
pivoted AS (
    SELECT 
        lender_entity_name,
        activity_year,
        MAX(CASE WHEN applicant_derived_racial_category = 'White' THEN CAST(approved_apps AS DOUBLE) / total_apps END) AS white_approval_rate,
        MAX(CASE WHEN applicant_derived_racial_category IN ('Black', 'Latinx', 'AAPI') THEN CAST(approved_apps AS DOUBLE) / total_apps END) AS minority_approval_rate
    FROM lender_year_race
    GROUP BY lender_entity_name, activity_year
),
final AS (
    SELECT *,
           white_approval_rate - minority_approval_rate AS lender_minority_gap
    FROM pivoted
)

SELECT * FROM final
