-- models/stg_hmda_training_sampled.sql

{{ config(
    materialized = 'table',
    post_hook = "CREATE INDEX IF NOT EXISTS idx_strata_bin ON {{ this }}(strata_bin)"
) }}

WITH stratified_sample AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY loan_approved ORDER BY RANDOM()) AS row_num,
            COUNT(*) OVER (PARTITION BY loan_approved) AS total_rows
        FROM {{ ref('stg_hmda_training_with_strata') }}
        WHERE loan_approved IS NOT NULL
    )
    WHERE row_num <= total_rows * 0.10
)

SELECT *
FROM stratified_sample
