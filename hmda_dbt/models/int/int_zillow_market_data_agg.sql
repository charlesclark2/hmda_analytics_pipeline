{{
    config(
        materialized='table'
    )
}}

with zillow_base as (
    select
        zip_code,
        extract(year from period_begin) as year,
        median_sale_price,
        median_sale_price_year_over_year,
        median_price_per_square_foot,
        median_price_per_square_foot_year_over_year,
        median_list_price, 
        median_list_price_year_over_year,
        inventory,
        new_listings,
        median_days_on_market
    from {{ ref('stg_zillow_market_data') }}
),

zillow_agg as (
    select
        zip_code,
        year,
        avg(median_sale_price) as avg_median_sale_price,
        avg(median_sale_price_year_over_year) as avg_median_sale_price_year_over_year,
        avg(median_price_per_square_foot) as avg_median_price_per_square_foot,
        avg(median_price_per_square_foot_year_over_year) as avg_median_price_per_square_foot_year_over_year,
        avg(median_list_price) as avg_median_list_price, 
        avg(median_list_price_year_over_year) as avg_median_list_price_year_over_year,
        avg(inventory) as avg_inventory,
        avg(new_listings) as avg_new_listings,
        avg(median_days_on_market) as avg_median_days_on_market
    from zillow_base
    group by zip_code, year
)

select * from zillow_agg
