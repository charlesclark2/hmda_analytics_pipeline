{{
    config(
        materialized='table'
    )
}}

with base as (
    select 
        s.loan_application_id, 
        p.census_tract,
        p.county_code,
        s.applicant_derived_racial_category as race, 
        s.actual_value, 
        s.score
    from {{ source('other', 'scored_results') }} s 
    join {{ ref('mart_hmda_preprocessed') }} p 
    on s.loan_application_id = p.loan_application_id
    where race not like 'Unknown'
    and p.census_tract not in ('000000000Na', '000000000nA')
), 
score_aggregates as (
    select 
        census_tract, 
        max(county_code) county_code,
        race, 
        count(*) as total_apps, 
        avg(score) as avg_score, 
        sum(case when actual_value = 1 then 1 else 0 end) as actual_positives, 
        sum(case when actual_value = 0 then 1 else 0 end) as actual_denials,
        avg(case when actual_value = 1 then score end) as avg_score_given_positive, 
        sum(case when score >= 0.5 then 1 else 0 end) as predicted_positives
    from base 
    group by census_tract, race 
), 
white_reference as (
    select 
        census_tract, 
        avg_score as avg_score_white, 
        avg_score_given_positive as avg_score_pos_white
    from score_aggregates
    where race ilike 'white'
), 
metrics as (
    select 
        s.census_tract, 
        s.county_code, 
        sx.zip_code,
        coalesce(sx.state_name, sf.usps) as state_name,
        sx.city_name,
        tc.centroid_lat as latitude, 
        tc.centroid_lon as longitude,
        s.race, 
        s.total_apps, 
        s.actual_positives, 
        s.predicted_positives,
        s.actual_denials,
        s.avg_score, 
        s.avg_score_given_positive, 
        w.avg_score_white, 
        w.avg_score_pos_white, 
        s.actual_positives / nullif(s.total_apps, 0) as actual_approval_rate,
        s.predicted_positives / nullif(s.total_apps, 0) as predicted_approval_rate, 
        (s.predicted_positives / nullif(s.total_apps, 0)) - actual_approval_rate as model_vs_actual_gap,
        s.avg_score / nullif(w.avg_score_white, 0) as score_dpd, 
        s.avg_score_given_positive / nullif(w.avg_score_pos_white, 0) as score_eod, 
        case 
            when s.race in ('Latinx', 'Black', 'AAPI')
                and (score_dpd < 0.8 or score_eod < 0.8) then 1 
            else 0 
        end as is_possible_redlining, 
        case 
            when s.race in ('Latinx', 'Black', 'AAPI')
                and (score_dpd < 0.8 and score_eod < 0.8) then 1 
            else 0 
        end as is_redlining
    from score_aggregates s 
    left join white_reference w 
        on s.census_tract = w.census_tract
    left join {{ ref('stg_census_tract_to_zipcode') }} sx 
        on s.census_tract = sx.census_tract
    left join {{ source('other', 'state_fips_xref')}} sf 
        on sf.fips::varchar = substr(s.census_tract, 1, 2)::varchar
    left join {{ source('other', 'tract_centroids_raw') }} tc 
        on s.census_tract = tc.tract_fips

    qualify row_number() over (partition by s.census_tract, s.race order by s.total_apps desc) = 1
)
select *
from metrics

