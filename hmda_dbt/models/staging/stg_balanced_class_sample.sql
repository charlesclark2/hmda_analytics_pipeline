{{
    config(
        materialized='table'
    )
}}

WITH approved AS (
    SELECT *
    FROM {{ ref('mart_hmda_preprocessed') }}
    WHERE loan_approved = 1
    LIMIT 3000000
),
denied AS (
    SELECT *
    FROM {{ ref('mart_hmda_preprocessed') }}
    WHERE loan_approved = 0
    LIMIT 3000000
), 
unioned as (
    SELECT * FROM approved
    UNION ALL
    SELECT * FROM denied
)
select *
from unioned 
order by activity_year, loan_purpose_grouped

