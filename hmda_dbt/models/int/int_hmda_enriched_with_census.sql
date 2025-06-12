{{
    config(
        materialized='table'
    )
}}

with hmda as (
    select 
        *
    from {{ ref('mart_hmda_applications_filtered') }}
), 
census as (
    select *
    from {{ ref('stg_census_acs') }}
), 
income_buckets as (
    select *
    from {{ ref('int_census_income_buckets') }}
), 
joined as (
    select 
        h.*,
        c.median_household_income, 
        c.white_population_pct, 
        c.black_population_pct, 
        c.latinx_population_pct,
        c.minority_population_pct, 
        c.bachelors_degree_population_pct, 
        c.graduate_degree_population_pct, 
        c.pct_bachelors_or_higher,
        c.total_population_below_poverty_level_pct, 
        c.total_population_receiving_federal_assistance_pct, 
        c.gini_index_of_income_inequality, 
        b.income_bucketed
    from hmda h
    left join census c 
        on h.census_tract = c.census_tract
    left join income_buckets b 
        on h.census_tract = b.census_tract 
)
select *
from joined
where census_tract is not null 
