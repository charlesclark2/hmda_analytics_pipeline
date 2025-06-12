{{ 
    config(
        materialized='table', 
        schema='refined'
    )
}}

-- set variables for ease of use 
{% set acs_columns = ['median_household_income', 'minority_population_pct', 
              'pct_bachelors_or_higher', 'total_population_below_poverty_level_pct', 
              'gini_index_of_income_inequality', 'income_bucketed'] %}

{% set zillow_columns = [
    'avg_median_sale_price', 'avg_median_sale_price_year_over_year', 'avg_median_list_price', 
    'avg_median_list_price_year_over_year', 'avg_median_price_per_square_foot', 
    'avg_median_price_per_square_foot_year_over_year', 'avg_inventory', 
    'avg_new_listings', 'avg_median_days_on_market'
]%}

{% set log_transform_columns = [
    'income', 'loan_amount', 'property_value', 'median_household_income',
    'avg_median_sale_price', 'avg_median_list_price','avg_median_price_per_square_foot', 
    'avg_inventory','avg_new_listings', 'avg_median_days_on_market',
    'avg_median_sale_price_year_over_year', 'avg_median_list_price_year_over_year', 
    'avg_median_price_per_square_foot_year_over_year', 'total_population_below_poverty_level_pct'
]%}

{% set secondary_quantile_columns = [
    'total_population_below_poverty_level_pct', 'avg_median_days_on_market'
]%}

{% set additional_transform_columns = [
    'loan_to_income_ratio', 'income_to_loan_ratio_stratified'
]%}

with base as (
    select *
    from {{ ref('mart_hmda_model_features') }}
), 
cleaned as (
    select 
        b.loan_application_id, 
        cast(b.activity_year::varchar as integer) as activity_year,
        cast(b.loan_approved::varchar as integer) as loan_approved,
        b.loan_purpose, 
        case 
            when b.loan_purpose = 1 then 'Home Purchase'
            when b.loan_purpose = 2 then 'Home Improvement'
            when b.loan_purpose in (31, 32) then 'Refinancing'
            when b.loan_purpose = 4 then 'Other'
            else 'Not Applicable'
        end as loan_purpose_grouped,
        b.loan_type, 
        b.occupancy_type,
        cast(b.state_code::varchar as varchar) as state_code, 
        cast(b.county_code::varchar as varchar) as county_code, 
        cast(b.census_tract::varchar as varchar) as census_tract,
        cast(b.zip_code::varchar as varchar) as zip_code, 
        -- lender information 
        b.lender_entity_name,
        b.lender_entity_age, 
        case 
            when upper(b.lender_registration_status) in ('RETIRED', 'DUPLICATE', 'ERROR')
                then 'OTHER'
            else upper(b.lender_registration_status)
        end as lender_registration_status,
        case 
            -- complete necessary groupings
            when b.applicant_sex_category in ('Female', 'Male')
                then b.applicant_sex_category
            when b.applicant_sex_category like 'information not provided'
                then 'Unknown'
            else 'Other'
        end as applicant_sex_category, 
        case 
            when b.applicant_age is null then 'Unknown'
            else cast(b.applicant_age as varchar)
        end as applicant_age, 
        case 
            when b.applicant_derived_racial_category is null then 'Unknown'
            else b.applicant_derived_racial_category
        end as applicant_derived_racial_category,
        case
            -- Handle NULLs
            when b.debt_to_income_ratio is null then null

            -- Strip out values like "<20", "30-35%", etc.
            when regexp_like(b.debt_to_income_ratio, '[<%-]') then 
                try_cast(regexp_substr(b.debt_to_income_ratio, '\\d+', 1, 1) as double)

            -- Attempt direct cast for numeric strings
            when try_cast(b.debt_to_income_ratio as double) is not null then 
                try_cast(b.debt_to_income_ratio as double)

            -- Fallback for anything unparseable
            else null
        end as cleaned_dti, 
        b.property_value,
        b.loan_amount,
        b.income, 
        -- acs columns 
        {%- for column in acs_columns %}
            {% if column == 'gini_index_of_income_inequality' %}
                iff(b.{{ column }} < 0, null, b.{{ column }}) as {{ column }},
            {% elif column == 'income_bucketed' %}
                round(b.{{ column }}, 0) as {{ column }},
            {% else %}
                b.{{ column }},
            {% endif %}
        {% endfor -%}
        iff(b.is_distressed is null, 0, cast(b.is_distressed as integer)) as is_distressed, 
        iff(b.is_underserved is null, 0, cast(b.is_underserved as integer)) as is_underserved,
        -- zillow column 
        {% for column in zillow_columns %}
        b.{{ column }},
        {% endfor %}
        -- other feature engineering columns 
        -- race/sex interaction
        coalesce(cast(b.applicant_derived_racial_category as varchar), 'Unknown') || '_' ||
        coalesce(cast(b.applicant_sex_category as varchar), 'Unknown') as race_sex_interaction,
        -- race/geography interaction 
        coalesce(cast(b.applicant_derived_racial_category as varchar), 'Unknown') || '_' ||
        coalesce(cast(b.state_code as varchar), 'Unknown') as race_state_interaction,
        -- missing values flags
        iff(b.property_value is not null, 1, 0) as has_property_value, 
        iff(b.median_household_income is not null, 1, 0) as has_acs_data, 
        iff(b.gini_index_of_income_inequality >= 0, 1, 0) as has_gini_data
    from base b 
), 
filtered as (
    select *, 
        -- add feature for combined distessed_underserved
        case 
            when is_distressed = 1 or is_underserved = 1 then 1 
            else 0 
        end as is_distressed_or_underserved, 
        -- add feature for combining distressed/underserved with applicant race
        case 
            when is_distressed = 1 or is_underserved = 1 
                then 'DistressedOrUnderserved_' || coalesce(applicant_derived_racial_category, 'Unknown')
            else 'NotDistressedOrUnderserved_' || coalesce(applicant_derived_racial_category, 'Unknown')
        end as distressed_or_underserved_race
    from cleaned 
    -- filter out where loan_purpose_grouped isn't applicable
    where loan_purpose_grouped != 'Not Applicable'
),
global_median_values as (
    select 
        median(cleaned_dti) as median_dti, 
        median(property_value) as median_property_value, 
        median(loan_amount) as median_loan_amount,
        {% for column in acs_columns %}
            median({{ column }}) as median_{{ column }},
        {% endfor %}
        {% for column in zillow_columns %}
            median({{ column }}) as median_{{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from filtered 
), 
zip_and_purpose_partitioned_median_values as (
    select 
        zip_code, 
        loan_purpose,
        median(property_value) as median_property_value
    from filtered 
    where loan_purpose is not null 
    and zip_code is not null
    group by loan_purpose, zip_code 
),
zip_code_partitioned_median_values as (
    select 
        zip_code,
        median(property_value) as median_property_value, 
        {% for column in acs_columns %}
            median({{ column }}) as median_{{ column }},
        {% endfor %}
        {% for column in zillow_columns %}
            median({{ column }}) as median_{{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from filtered 
    where zip_code is not null 
    group by zip_code 
),
loan_purpose_median_values as (
    select 
        loan_purpose, 
        median(property_value) as median_property_value 
    from filtered 
    where loan_purpose is not null 
    group by loan_purpose 
),
state_and_county_median_values as (
    select 
        state_code, 
        county_code, 
        {% for column in acs_columns %}
            median({{ column }}) as sc_med_{{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from filtered 
    where state_code is not null 
    and county_code is not null 
    group by state_code, county_code
),
state_median_values as (
    select 
        state_code, 
        {% for column in acs_columns %}
            median({{ column }}) as st_med_{{ column }},
        {% endfor %}
        {% for column in zillow_columns %}
            median({{ column }}) as st_med_{{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from filtered 
    where state_code is not null 
    group by state_code
),
-- feature engineering: high debt to income and large loan 
state_purpose_medians as (
    select 
        state_code, 
        loan_purpose_grouped, 
        median(loan_amount) as median_loan_amount
    from filtered 
    where state_code is not null 
    and loan_purpose_grouped is not null 
    and loan_amount is not null 
    group by state_code, loan_purpose_grouped
),
-- initial capping
caps as (
    select 
        {% for column in log_transform_columns %}
            approx_percentile({{ column }}, 0.01) as {{ column }}_lower_cap, 
            approx_percentile({{ column }}, 0.99) as {{ column }}_upper_cap{% if not loop.last %},{% endif %}
        {% endfor %}
    from filtered 
),
-- feature engineering: prior activity
tract_year_summary as (
    select 
        census_tract, 
        activity_year, 
        sum(loan_approved) as approvals, 
        count(loan_approved) as total_apps
    from filtered 
    group by census_tract, activity_year
),
prior_summary as (
    select 
        census_tract, 
        activity_year, 
        sum(approvals) over (
            partition by census_tract
            order by activity_year
            rows between unbounded preceding and 1 preceding
        ) as hist_approvals,
        sum(total_apps) over (
            partition by census_tract 
            order by activity_year
            rows between unbounded preceding and 1 preceding
        ) as hist_total_apps
    from tract_year_summary
),
prior_approval_rate as (
    select 
        census_tract, 
        activity_year, 
        case 
            when hist_total_apps > 0 then hist_approvals * 1.0 / hist_total_apps
            else null 
        end as prior_approval_rate
    from prior_summary
),
median_approval_rates as (
    select 
        activity_year, 
        median(prior_approval_rate) as median_prior_rate
    from prior_approval_rate
    where activity_year != 2018
    and prior_approval_rate is not null 
    group by activity_year 
),
-- feature engineering - race/sex interactions
race_sex_interaction_counts as (
    select 
        race_sex_interaction, 
        count(*) as cnt 
    from filtered 
    group by race_sex_interaction
),
race_sex_consolidated as (
    select 
        race_sex_interaction, 
        case 
            when cnt < 5000 then 'Other'
            else race_sex_interaction 
        end as race_sex_interaction_consolidated
    from race_sex_interaction_counts
),
-- feature engineering: stratified income to loan ratio
loan_purpose_grouped_medians as (
    select 
        loan_purpose_grouped, 
        median(loan_amount) as median_loan_amount
    from filtered 
    group by loan_purpose_grouped
),
-- imputation and first layer transformations
imputed as (
    select 
        c.loan_application_id, 
        c.activity_year, 
        c.loan_approved,
        c.state_code, 
        c.county_code, 
        c.census_tract, 
        c.zip_code, 
        -- lender information
        c.lender_entity_name, 
        c.lender_entity_age, 
        c.lender_registration_status,
        -- applicant information
        c.applicant_derived_racial_category,
        c.applicant_sex_category, 
        c.applicant_age, 
        c.income, 
        coalesce(
            c.cleaned_dti, 
            g.median_dti 
        ) as debt_to_income_ratio, 
        -- loan information 
        c.loan_purpose, 
        c.loan_purpose_grouped,
        cast(c.loan_type as varchar) as loan_type,
        cast(c.occupancy_type as varchar) as occupancy_type,
        coalesce(
            c.property_value, 
            zpm.median_property_value, 
            zcm.median_property_value, 
            lpm.median_property_value, 
            g.median_property_value
        ) as property_value,
        c.loan_amount, 
        -- acs values 
        {% for column in acs_columns %}
            coalesce(
                c.{{ column }}, 
                scm.sc_med_{{ column }}, 
                smv.st_med_{{ column }}, 
                g.median_{{ column }}
            ) as {{ column }},
        {% endfor %}
        {% for column in zillow_columns %}
            coalesce(
                c.{{ column }}, 
                zcm.median_{{ column }}, 
                smv.st_med_{{ column }}, 
                g.median_{{ column }}
            ) as {{ column }},
        {% endfor %}
        -- log transformed columns
        {% for column in log_transform_columns %}
            case 
                when c.{{ column }} is not null and c.{{ column }} >= -1
                    then ln(1 + cast(c.{{ column }} as double))
                else ln(1 + 0)
            end as {{ column }}_log,
            iff(c.{{ column }} is null, 1, 0) as {{ column }}_log_missing_flag,
        {% endfor %}
        -- capped columns 
        {% for column in log_transform_columns  %}
            case 
                when c.{{ column }} is null 
                    then cps.{{ column }}_lower_cap
                when c.{{ column }} < cps.{{ column }}_lower_cap 
                    then cps.{{ column }}_lower_cap 
                when c.{{ column }} > cps.{{ column }}_upper_cap 
                    then cps.{{ column }}_upper_cap
                else c.{{ column }}
            end as {{ column }}_capped, 
        {% endfor %}
        c.is_distressed, 
        c.is_underserved,
        c.is_distressed_or_underserved,
        c.has_property_value, 
        c.has_acs_data, 
        c.has_gini_data, 
        iff(pa.prior_approval_rate is not null, 1, 0) as has_prior_approval_rate_data,
        -- feature engineering columns
        case 
            when pa.prior_approval_rate is not null then pa.prior_approval_rate
            when pa.activity_year = 2018 then 0.5
            when pa.activity_year != 2018 then mar.median_prior_rate
            else null 
        end as prior_approval_rate, 
        rsc.race_sex_interaction_consolidated as race_sex_interaction, 
        c.race_state_interaction,
        case 
            when lpgm.median_loan_amount is not null and lpgm.median_loan_amount != 0 
                then 
                    case 
                        when c.loan_amount / lpgm.median_loan_amount != 0
                            then c.income / (c.loan_amount / lpgm.median_loan_amount)
                        else null 
                    end 
            when c.loan_amount != 0 then c.income / c.loan_amount 
            else null 
        end as income_to_loan_ratio_stratified, 
        -- loan amount to local median income ratio
        case 
            when coalesce(c.median_household_income, g.median_median_household_income) > 0
                then coalesce(c.loan_amount, g.median_loan_amount) / coalesce(c.median_household_income, g.median_median_household_income)
                else null 
        end as loan_to_income_ratio, 
        coalesce(
            spm.median_loan_amount, 
            g.median_loan_amount
        ) as loan_amount_threshold, 
        c.distressed_or_underserved_race
    from filtered c 
    cross join global_median_values g 
    cross join caps cps 
    left join zip_and_purpose_partitioned_median_values zpm
        on c.loan_purpose = zpm.loan_purpose 
        and c.zip_code = zpm.zip_code 
    left join zip_code_partitioned_median_values zcm 
        on c.zip_code = zcm.zip_code 
    left join loan_purpose_median_values lpm 
        on c.loan_purpose = lpm.loan_purpose
    left join state_and_county_median_values scm 
        on c.state_code = scm.state_code 
        and c.county_code = scm.county_code 
    left join state_median_values smv 
        on c.state_code = smv.state_code 
    left join prior_approval_rate pa 
        on c.census_tract = pa.census_tract
        and c.activity_year = pa.activity_year
    left join median_approval_rates mar 
        on pa.activity_year = mar.activity_year
    left join race_sex_consolidated rsc 
        on c.race_sex_interaction = rsc.race_sex_interaction
    left join loan_purpose_grouped_medians lpgm 
        on c.loan_purpose_grouped = lpgm.loan_purpose_grouped 
    left join state_purpose_medians spm 
        on c.state_code = spm.state_code 
        and c.loan_purpose_grouped = spm.loan_purpose_grouped
),
secondary_quantiles as (
    select 
        {% for column in secondary_quantile_columns %}
            approx_percentile({{ column }}_log, 0.25) as {{ column }}_q1, 
            approx_percentile({{ column }}_log, 0.5) as {{ column }}_q2, 
            approx_percentile({{ column }}_log, 0.75) as {{ column }}_q3{% if not loop.last %},{% endif %}
        {% endfor %}
    from imputed 
),
secondary_base as (
    select 
        i.*, 
        (select approx_percentile(income_to_loan_ratio_stratified, 0.01) from imputed) as income_to_loan_ratio_stratified_lower_cap,
        (select approx_percentile(income_to_loan_ratio_stratified, 0.99) from imputed) as income_to_loan_ratio_stratified_upper_cap,
        (select approx_percentile(loan_to_income_ratio, 0.01) from imputed) as loan_to_income_ratio_lower_cap,
        (select approx_percentile(loan_to_income_ratio, 0.99) from imputed) as loan_to_income_ratio_upper_cap
    from imputed i 
),
secondary_caps as (
    select 
        c.*, 
        {% for column in additional_transform_columns  %}
            case 
                when c.{{ column }} is null 
                    then c.{{ column }}_lower_cap
                when c.{{ column }} < c.{{ column }}_lower_cap 
                    then c.{{ column }}_lower_cap 
                when c.{{ column }} > c.{{ column }}_upper_cap 
                    then c.{{ column }}_upper_cap
                else c.{{ column }}
            end as {{ column }}_capped{% if not loop.last %},{% endif %}
        {% endfor %}
    from secondary_base c
),
secondary_transformations as (
    select 
        i.*, 
        {% set secondary_transform_columns = log_transform_columns + additional_transform_columns %}
        {% for column in secondary_transform_columns %}
        case 
            when i.{{ column }}_capped is not null and i.{{ column }}_capped >= -1
                then ln(1 + cast(i.{{ column }}_capped as double))
            else ln(1 + 0)
        end as {{ column }}_capped_and_log,
        iff(i.{{ column }}_capped is null, 1, 0) as {{ column }}_capped_and_log_missing_flag{% if not loop.last %},{% endif %}
        {% endfor %}
    from secondary_caps i 
), 
binned as (
    select 
        st.*, 
        case 
            when st.debt_to_income_ratio >= 0 and st.debt_to_income_ratio < 30 then 'Low'
            when st.debt_to_income_ratio >= 30 and st.debt_to_income_ratio < 40 then 'Medium'
            when st.debt_to_income_ratio >=40 and st.debt_to_income_ratio < 50 then 'High'
            when st.debt_to_income_ratio >= 50 and st.debt_to_income_ratio < 100 then 'Very High'
            else null 
        end as dti_bin, 
        case 
            when st.minority_population_pct < 20 then 'Low Minority'
            when st.minority_population_pct < 50 then 'Moderate Minority'
            when st.minority_population_pct < 80 then 'High Minority'
            else 'Very High Minority'
        end as minority_pct_bin,
        case 
            when st.gini_index_of_income_inequality < 0.3 then 'Low Inequality'
            when st.gini_index_of_income_inequality < 0.5 then 'Moderate Inequality'
            else 'High Inequality'
        end as gini_bin,
        -- feature engineering - high dti and large loan 
        case 
            when st.debt_to_income_ratio >= 40 and st.loan_amount > st.loan_amount_threshold
                then 1 
            else 0 
        end as high_dti_large_loan,
        {% for column in secondary_quantile_columns %} 
        case 
            when st.{{ column }}_log < sq.{{ column }}_q1 then 'Low'
            when st.{{ column }}_log < sq.{{ column }}_q2 then 'Med-Low'
            when st.{{ column }}_log < sq.{{ column }}_q3 then 'Med-High'
            else 'High'
        end as {{ column }}_log_bin{% if not loop.last %},{% endif %}
        {% endfor %}
    from secondary_transformations st
    cross join secondary_quantiles sq 
), 
dedupe as (
select *, 
row_number() over (order by loan_application_id) as rn 
from binned 
)
select *
from dedupe 
