with staging as (
    select *
    from {{ ref('stg_census_acs') }}
), 
bucketed as (
    select 
        census_tract, 
        median_household_income,
        ntile(4) over (order by median_household_income) as income_bucketed
    from staging 
)
select *
from bucketed