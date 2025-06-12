with raw_data as (
    select *
    from {{ source('other', 'distressed_census_tracts_raw')}}
)
select
    report_year,
    tract_fips as census_tract, 
    is_distressed, 
    is_underserved
from raw_data
