{{
    config(
        materialized='view'
    )
}}

with raw as (
    select *
    from {{ source('other', 'census_acs_raw') }}
), 
transformed as (
    select 
        name, 
        total_population, 
        white_population,
        case 
            when total_population > 0 then round((white_population / total_population) * 100, 2) 
            else null 
        end as white_population_pct,

        black_population,
        case 
            when total_population > 0 then round((black_population / total_population) * 100, 2) 
            else null 
        end as black_population_pct, 

        latinx_population,
        case 
            when total_population > 0 then round((latinx_population / total_population) * 100, 2) 
            else null 
        end as latinx_population_pct,

        total_population - white_population as minority_population,
        case 
            when total_population > 0 then round(((total_population - white_population) / total_population) * 100, 2) 
            else null 
        end as minority_population_pct,

        case 
            when median_household_income < 0 then null 
            else median_household_income
        end as median_household_income,

        median_home_value,

        bachelors_degree_population,
        case 
            when total_population > 0 then round((bachelors_degree_population / total_population) * 100, 2) 
            else null 
        end as bachelors_degree_population_pct,

        graduate_degree_population,
        case 
            when total_population > 0 then round((graduate_degree_population / total_population) * 100, 2) 
            else null 
        end as graduate_degree_population_pct,

        high_school_diploma_population,
        case 
            when total_population > 0 then round((high_school_diploma_population / total_population) * 100, 2) 
            else null 
        end as high_school_diploma_population_pct,

        case 
            when total_population > 0 then round(((bachelors_degree_population + graduate_degree_population) / total_population) * 100, 2) 
            else null 
        end as pct_bachelors_or_higher,

        total_population_below_poverty_level,
        case 
            when total_population > 0 then round((total_population_below_poverty_level / total_population) * 100, 2) 
            else null 
        end as total_population_below_poverty_level_pct,

        total_population_receiving_federal_assistance,
        case 
            when total_population > 0 then round((total_population_receiving_federal_assistance / total_population) * 100, 2) 
            else null 
        end as total_population_receiving_federal_assistance_pct,

        gini_index_of_income_inequality,

        lpad(state, 2, '0') as state_fips,
        lpad(county, 3, '0') as county_fips,
        lpad(tract, 6, '0') as tract_fips,
        concat(lpad(state, 2, '0'), lpad(county, 3, '0'), lpad(tract, 6, '0')) as census_tract
    from raw
)

select *
from transformed
