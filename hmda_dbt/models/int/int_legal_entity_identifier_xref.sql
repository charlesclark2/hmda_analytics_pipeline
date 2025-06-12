{{
    config(
        materialized='table'
    )
}}

with base as (
    select *
    from {{ ref('stg_legal_entity_identifier') }}
), 
transformed as (
    select 
        lei, 
        upper(entity_name) as entity_name, 
        upper(registration_status) as registration_status, 
        registration_initial_date::date as registration_initial_date, 
        datediff(
            year, 
            registration_initial_date::date, 
            current_date()
        ) as entity_age,
        registration_last_update::date as registration_last_update
    from base 
)
select *
from transformed
where lei is not null
