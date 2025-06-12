{{ 
    config(
        materialized='table'
    )
}}

with base as (
    select 
        r.loan_application_id, 
        p.county_code, 
        p.census_tract, 
        r.original_race, 
        r.new_race, 
        r.previous_score, 
        r.counterfactual_score,
        r.score_change
    from {{ source('other', 'race_counterfactual_results') }} r
    join {{ ref('mart_hmda_preprocessed') }} p 
    on r.loan_application_id = p.loan_application_id
    where r.score_change is not null
      and p.census_tract not in ('000000000Na', '000000000nA')
), 
score_aggregates as (
    select 
        b.census_tract, 
        b.original_race, 
        b.new_race,
        max(b.county_code) as county_code, 
        count(*) as total_apps, 
        avg(b.previous_score) as avg_prev_score, 
        avg(b.counterfactual_score) as avg_counterfactual_score, 
        avg(b.score_change) as avg_score_change
    from base b 
    group by b.census_tract, b.original_race, b.new_race 
), 
centroids as (
    select 
        s.*, 
        t.centroid_lat as latitude, 
        t.centroid_lon as longitude
    from score_aggregates s 
    left join {{ source('other', 'tract_centroids_raw') }} t 
    on s.census_tract = t.tract_fips 
    qualify row_number() over (partition by s.census_tract, s.original_race, s.new_race order by s.total_apps desc) = 1
)
select *
from centroids 