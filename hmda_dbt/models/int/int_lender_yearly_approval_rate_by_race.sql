-- models/staging/stg_lender_yearly_approval_rates_by_race.sql

{{ 
  config(
    materialized='table', 
    tags=["remaining_models"]
  ) 

}}

SELECT
    lender_entity_name,
    activity_year,
    applicant_derived_racial_category,
    COUNT(*) AS total_applications,
    SUM(CASE WHEN loan_approved = 1 THEN 1 ELSE 0 END) AS approvals,
    1.0 * SUM(CASE WHEN loan_approved = 1 THEN 1 ELSE 0 END) / COUNT(*) AS approval_rate
FROM {{ ref('mart_hmda_preprocessed') }}
WHERE lender_entity_name IS NOT NULL
  AND loan_approved IS NOT NULL
  AND applicant_derived_racial_category IS NOT NULL
GROUP BY
    lender_entity_name,
    activity_year,
    applicant_derived_racial_category
