{{ 
    config(
        materialized='table'
    )
}}

select 
    *, 
    concat(
        cast(activity_year as varchar), '_', 
        loan_purpose_grouped, '-', 
        cast(loan_approved as varchar)
    ) as strata_bin
from {{ ref('mart_hmda_preprocessed') }}
where loan_approved is not null 
order by strata_bin
