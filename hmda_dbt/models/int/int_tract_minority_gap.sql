{{ 
    config(
        materialized='table', 
        tags=["remaining_models"]
    ) 
}}

WITH tract_year_race AS (
    SELECT 
        census_tract,
        activity_year,
        applicant_derived_racial_category,
        COUNT(*) AS total_apps,
        SUM(CASE WHEN loan_approved = 1 THEN 1 ELSE 0 END) AS approved_apps
    FROM {{ ref('mart_hmda_preprocessed') }}
    WHERE loan_approved IS NOT NULL
    GROUP BY census_tract, activity_year, applicant_derived_racial_category
),
pivoted AS (
    SELECT 
        census_tract,
        activity_year,
        MAX(CASE WHEN applicant_derived_racial_category = 'White' THEN CAST(approved_apps AS DOUBLE) / total_apps END) AS white_approval_rate,
        MAX(CASE WHEN applicant_derived_racial_category IN ('Black', 'Latinx', 'AAPI') THEN CAST(approved_apps AS DOUBLE) / total_apps END) AS minority_approval_rate
    FROM tract_year_race
    GROUP BY census_tract, activity_year
),
final AS (
    SELECT *,
           white_approval_rate - minority_approval_rate AS tract_minority_gap
    FROM pivoted
)

SELECT * FROM final
