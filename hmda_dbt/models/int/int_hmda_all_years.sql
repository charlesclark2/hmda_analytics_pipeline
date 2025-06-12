{{
    config(
        materialized='table'
    )
}}

with combined as (

    select * from {{ ref('stg_hmda_snapshot_raw_2018') }}
    union all
    select * from {{ ref('stg_hmda_snapshot_raw_2019') }}
    union all
    select * from {{ ref('stg_hmda_snapshot_raw_2020') }}
    union all
    select * from {{ ref('stg_hmda_snapshot_raw_2021') }}
    union all
    select * from {{ ref('stg_hmda_snapshot_raw_2022') }}
    union all
    select * from {{ ref('stg_hmda_snapshot_raw_2023') }}

)

select * from combined
