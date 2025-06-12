{{ 
    config(
        materialized='table', 
        tags=["remaining_models"]
    ) 
}}

-- Prior-year approval rates per tract and racial group
WITH tract_race_year AS (
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
lagged AS (
    SELECT 
        census_tract,
        activity_year,
        applicant_derived_racial_category,
        CAST(approved_apps AS DOUBLE) / total_apps AS tract_approval_rate
    FROM tract_race_year
)

SELECT * FROM lagged
