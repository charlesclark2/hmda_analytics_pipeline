{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT
        CAST(actual_value AS INT) AS actual,
        CAST(predicted_label AS INT) AS predicted
    FROM {{ source('other', 'scored_results') }}
),

confusion_matrix AS (
    SELECT
        COUNT_IF(actual = 1 AND predicted = 1) AS tp,
        COUNT_IF(actual = 0 AND predicted = 1) AS fp,
        COUNT_IF(actual = 1 AND predicted = 0) AS fn,
        COUNT(*) AS total,
        COUNT_IF(actual = predicted) AS correct
    FROM base
),

metrics AS (
    SELECT
        c.correct::FLOAT / total AS accuracy,
        2.0 * c.tp / NULLIF((2 * c.tp + c.fp + c.fn), 0) AS f1_score
    FROM confusion_matrix c 
)
SELECT m.*, mp.auc_roc
FROM metrics m 
cross join (
    select activity_year, auc_roc 
    from {{ source('other', 'metrics_model_performance') }}
    where activity_year is null 
) mp 
