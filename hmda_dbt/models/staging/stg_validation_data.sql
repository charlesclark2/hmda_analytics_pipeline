{{ 
    config(
        materialized='table', 
        tags=["remaining_models"]
    )
}}

with base_2018 as (
    select *
    from {{ ref('mart_hmda_preprocessed') }}
    where activity_year = 2018
    order by random()
    limit 100000
), 
base_2019 as (
    select *
    from {{ ref('mart_hmda_preprocessed') }}
    where activity_year = 2019
    order by random()
    limit 100000
), 
base_2020 as (
    select *
    from {{ ref('mart_hmda_preprocessed') }}
    where activity_year = 2020
    order by random()
    limit 100000
), 
base_2021 as (
    select *
    from {{ ref('mart_hmda_preprocessed') }}
    where activity_year = 2021
    order by random()
    limit 100000
), 
base_2022 as (
    select *
    from {{ ref('mart_hmda_preprocessed') }}
    where activity_year = 2022
    order by random()
    limit 100000
), 
base_2023 as (
    select *
    from {{ ref('mart_hmda_preprocessed') }}
    where activity_year = 2023
    order by random()
    limit 100000
)
select * from base_2018
union all 
select * from base_2019
union all 
select * from base_2020 
union all 
select * from base_2021 
union all 
select * from base_2022 
union all 
select * from base_2023 