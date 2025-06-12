{{
    config(
        materialized='table'
    )
}}

with base as (
    select
        s.loan_application_id,
        s.applicant_derived_racial_category,
        s.applicant_age,
        s.actual_value,
        s.predicted_label,
        s.score,
        p.activity_year
    from {{ source('other', 'scored_results') }} s 
    join (
        select 
            loan_application_id, 
            activity_year 
        from {{ ref('mart_hmda_post_model_set') }} 
    ) p 
    on s.loan_application_id = p.loan_application_id
),

grouped as (
    select
        activity_year,
        applicant_derived_racial_category,
        count(*) as n,
        sum(case when actual_value = 1 then 1 else 0 end) as n_actual_pos,
        sum(case when predicted_label = 1 then 1 else 0 end) as n_pred_pos,
        sum(case when actual_value = 1 and predicted_label = 1 then 1 else 0 end) as n_tp, 
        sum(case when actual_value = 0 and predicted_label = 1 then 1 else 0 end) as n_fp, 
        sum(case when actual_value = 0 then 1 else 0 end) as n_actual_neg
    from base
    group by activity_year, applicant_derived_racial_category
),

metrics as (
    select
        activity_year,
        applicant_derived_racial_category,
        n_actual_pos,
        n_actual_neg,
        -- actual rates
        (n_actual_pos * 1.0) / nullif(n, 0) as actual_approval_rates,
        -- Demographic Parity Difference (DPD)
        (n_pred_pos * 1.0) / nullif(n, 0) as predicted_approval_rate,

        -- Equal Opportunity Difference (EOD)
        (n_tp * 1.0) / nullif(n_actual_pos, 0) as true_positive_rate,

        -- FPR
        (n_fp * 1.0) / nullif(n_actual_neg, 0) as false_positive_rate,

        -- precision
        (n_tp * 1.0) / nullif(n_pred_pos, 0) as precision_rate, 

        -- f1
        2.0 * (n_tp * 1.0) / nullif((2 * n_tp + n_fp), 0) as f1_score,

        -- Wilson lower/upper bounds for predicted positive rate (DPD)
        predicted_approval_rate -
            1.96 * sqrt((predicted_approval_rate * (1 - predicted_approval_rate)) / nullif(n, 0)) as dpd_ci_lower,
        predicted_approval_rate +
            1.96 * sqrt((predicted_approval_rate * (1 - predicted_approval_rate)) / nullif(n, 0)) as dpd_ci_upper,

        -- Wilson lower/upper bounds for TPR (EOD)
        true_positive_rate -
            1.96 * sqrt((true_positive_rate * (1 - true_positive_rate)) / nullif(n_actual_pos, 0)) as eod_ci_lower,
        true_positive_rate +
            1.96 * sqrt((true_positive_rate * (1 - true_positive_rate)) / nullif(n_actual_pos, 0)) as eod_ci_upper

    from grouped
), 
white_metrics as (
    select 
        activity_year, 
        actual_approval_rates,
        true_positive_rate, 
        false_positive_rate, 
        true_positive_rate -
            1.96 * sqrt((true_positive_rate * (1 - true_positive_rate)) / nullif(n_actual_pos, 0)) as white_tpr_ci_lower,
        true_positive_rate +
            1.96 * sqrt((true_positive_rate * (1 - true_positive_rate)) / nullif(n_actual_pos, 0)) as white_tpr_ci_upper,

        false_positive_rate -
            1.96 * sqrt((false_positive_rate * (1 - false_positive_rate)) / nullif(n_actual_neg, 0)) as white_fpr_ci_lower,
        false_positive_rate +
            1.96 * sqrt((false_positive_rate * (1 - false_positive_rate)) / nullif(n_actual_neg, 0)) as white_fpr_ci_upper
    from metrics 
    where applicant_derived_racial_category ilike 'White'
)

select 
    m.*, 
    w.true_positive_rate as white_tpr, 
    w.false_positive_rate as white_fpr, 
    w.white_tpr_ci_lower, 
    w.white_tpr_ci_upper, 
    w.white_fpr_ci_lower, 
    w.white_fpr_ci_upper,
    (m.actual_approval_rates - w.actual_approval_rates) as actual_gap,
    (m.true_positive_rate - w.true_positive_rate) as tpr_gap, 
    (m.false_positive_rate - w.false_positive_rate) as fpr_gap, 
    case 
        when m.eod_ci_upper < w.white_tpr_ci_lower or m.eod_ci_lower > w.white_tpr_ci_upper 
        then true else false 
    end as is_tpr_gap_significant,

    case 
        when m.false_positive_rate +
                    1.96 * sqrt((m.false_positive_rate * (1 - m.false_positive_rate)) / nullif(m.n_actual_neg, 0)) < w.white_fpr_ci_lower
            or 
                m.false_positive_rate -
                    1.96 * sqrt((m.false_positive_rate * (1 - m.false_positive_rate)) / nullif(m.n_actual_neg, 0)) > w.white_fpr_ci_upper
        then true else false 
    end as is_fpr_gap_significant
from metrics m 
join white_metrics w 
on m.activity_year = w.activity_year 
where m.applicant_derived_racial_category not like 'Unknown'
order by activity_year asc, applicant_derived_racial_category