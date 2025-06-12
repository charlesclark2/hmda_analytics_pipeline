
{{ 
    config(
        materialized='table', 
        order_by=['loan_approved', 'strata_bin']
    ) 
}}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_hmda_training_with_strata') }}
    WHERE loan_approved IS NOT NULL
),

randomized AS (
    SELECT
        *,
        RANDOM() AS sample_weight
    FROM base
)

SELECT *
FROM randomized
