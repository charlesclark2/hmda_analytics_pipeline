{{
    config(
        materialized='view'
    )
}}

with staging as (
    select *
    from {{ ref('stg_census_tract_to_zipcode') }}
), 
zillow as (
    select 
        *, 
        row_number() over (partition by zip_code order by year desc) as rn 
    from {{ ref('int_zillow_market_data_agg') }}
), 
zillow_dedupe as (
    select *
    from zillow 
    where rn = 1
), 
zips_with_zillow as (
    select distinct zip_code 
    from zillow_dedupe 
), 
ranked_zip_candidates as (
    select 
        m.census_tract, 
        m.zip_code, 
        m.city_name, 
        m.state_name, 
        m.residential_ratio, 
        case 
            when m.residential_ratio > 0 then 1 
            else 0
        end as has_residency_flag, 
        row_number() over (
            partition by m.census_tract 
            order by 
                has_residency_flag desc, 
                m.residential_ratio desc, 
                m.zip_code asc 
        ) as rn 
    from staging m
    join zips_with_zillow z 
        on m.zip_code = z.zip_code  
)
select 
    census_tract, 
    zip_code, 
    city_name, 
    state_name as state_postal_code, 
    residential_ratio
from ranked_zip_candidates
where rn = 1