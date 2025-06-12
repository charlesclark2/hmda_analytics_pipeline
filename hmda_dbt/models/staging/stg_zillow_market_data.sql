{{
    config(
        materialized='table'
    )
}}

with raw as (
    select *
    from {{ source('other', 'zillow_zip_code_market_tracker_raw') }}
), 
transformed as (
    select 
        try_to_date(period_begin, 'YYYY-MM-DD') as period_begin, 
        try_to_date(period_end, 'YYYY-MM-DD') as period_end, 

        -- Extract zip code from "region" (e.g., "zipcode:United States:53151")
        try_cast(split_part(trim(region), ':', 3) as varchar) as zip_code,

        state_code,

        -- Extract city from "parent_metro_region" (e.g., "Wisconsin, Milwaukee")
        try_cast(split_part(parent_metro_region, ',', 2) as varchar) as parent_metro_region_city,

        property_type, 
        property_type_id, 
        median_sale_price, 
        median_sale_price_yoy as median_sale_price_year_over_year, 
        median_list_price, 
        median_list_price_yoy as median_list_price_year_over_year, 
        median_ppsf as median_price_per_square_foot, 
        median_ppsf_yoy as median_price_per_square_foot_year_over_year,
        inventory, 
        new_listings, 
        median_dom as median_days_on_market, 
        homes_sold, 
        pending_sales, 
        price_drops, 
        avg_sale_to_list, 
        off_market_in_two_weeks
    from raw
    where property_type_id in (6, 13)
)
select *
from transformed 
where period_begin > '2018-01-01'
  and period_end < '2024-01-31'
