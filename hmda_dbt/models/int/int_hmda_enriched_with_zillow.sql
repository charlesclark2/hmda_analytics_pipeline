{{
    config(
        materialized='table'
    )
}}

with hmda as (
    select *
    from {{ ref('int_hmda_enriched_with_census') }}
), 
xref as (
    select 
        census_tract, 
        zip_code 
    from {{ ref('int_census_tract_to_zip_code_xref') }}
), 
zillow as (
    select *
    from {{ ref('int_zillow_market_data_agg') }}
), 
joined as (
    select
        h.*,
        try_cast(z.zip_code as varchar) as zip_code,
        z.avg_median_sale_price, 
        z.avg_median_sale_price_year_over_year, 
        z.avg_median_list_price, 
        z.avg_median_list_price_year_over_year, 
        z.avg_median_price_per_square_foot, 
        z.avg_median_price_per_square_foot_year_over_year, 
        z.avg_inventory, 
        z.avg_new_listings, 
        z.avg_median_days_on_market
    from hmda h 
    left join xref x 
        on h.census_tract = x.census_tract 
    left join zillow z 
        on try_cast(x.zip_code as varchar) = try_cast(z.zip_code as varchar)
        and h.activity_year = z.year
)
select *
from joined 

