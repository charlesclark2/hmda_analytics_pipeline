{{ 
    config(
        materialized='table'
    )
}}

with raw_data as (
    select *
    from {{ source('other', 'census_tract_to_zip_code_raw') }}
), 
transformed as (
    select 
        census_tract, 
        zip_code, 
        city_name, 
        state_name,
        residential_ratio
    from raw_data 
)
select *
from transformed
