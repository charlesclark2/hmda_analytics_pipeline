{{
    config(
        materialized='table'
    )
}}

with base as (
    select *
    from {{ ref('int_hmda_enriched_with_zillow') }}
), 
distress as (
    select 
        * 
    from {{ ref('stg_distressed_census_tracts') }}
), 
joined as (
    select 
        b.*, 
        d.is_distressed, 
        d.is_underserved
    from base b
    left join distress d
        on b.census_tract =  d.census_tract 
        and b.activity_year = d.report_year
)
select *
from joined
