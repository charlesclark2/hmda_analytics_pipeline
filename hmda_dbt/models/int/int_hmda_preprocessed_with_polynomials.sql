{{ config(materialized='table') }}

SELECT 
    *,
    income_log * property_value AS income_log_x_property_value,
    gini_index_of_income_inequality * income_log AS gini_x_income_log
FROM {{ ref('mart_hmda_preprocessed') }}
