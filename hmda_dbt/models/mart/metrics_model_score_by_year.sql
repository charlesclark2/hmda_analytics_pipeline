{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT
        p.activity_year,
        CAST(s.actual_value AS INT) AS actual,
        CAST(s.predicted_label AS INT) AS predicted
    FROM {{ source('other', 'scored_results') }} s 
    join {{ ref('mart_hmda_preprocessed') }} p 
    on s.loan_application_id = p.loan_application_id
),

confusion_matrix AS (
    SELECT
        activity_year,
        COUNT_IF(actual = 1 AND predicted = 1) AS tp,
        COUNT_IF(actual = 0 AND predicted = 1) AS fp,
        COUNT_IF(actual = 1 AND predicted = 0) AS fn,
        COUNT(*) AS total,
        COUNT_IF(actual = predicted) AS correct
    FROM base
    group by activity_year 
),

metrics AS (
    SELECT
        c.activity_year,
        m.auc_roc,
        c.correct::FLOAT / c.total AS accuracy,
        2.0 * c.tp / NULLIF((2 * c.tp + c.fp + c.fn), 0) AS f1_score
    FROM confusion_matrix c 
    join {{ source('other', 'metrics_model_performance') }} m 
    on m.activity_year = c.activity_year
)

SELECT * FROM metrics
order by activity_year asc