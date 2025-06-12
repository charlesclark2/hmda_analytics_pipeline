{{
    config(
        materialized='view'
    )
}}

with source as (
    select *
    from {{ source('other', 'legal_entity_identifier_raw') }}
), 
transformed as (
    select 
        lei, 
        entity_name, 
        registration_status, 
        registration_initial_date::date as registration_initial_date, 
        registration_last_update::date as registration_last_update
    from source 
)
select *
from transformed
