{{ 
  config(
    materialized = 'table', 
    tags=["remaining_models"]
  ) 
}}

SELECT 
    lender_entity_name,
    activity_year,
    COUNT(*) AS total_apps,
    SUM(CASE WHEN loan_approved = 1 THEN 1 ELSE 0 END) AS total_approved,
    CAST(SUM(CASE WHEN loan_approved = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS lender_prior_year_approval_rate
FROM {{ ref('mart_hmda_preprocessed') }}
WHERE lender_entity_name IS NOT NULL
  AND loan_approved IS NOT NULL
GROUP BY lender_entity_name, activity_year
