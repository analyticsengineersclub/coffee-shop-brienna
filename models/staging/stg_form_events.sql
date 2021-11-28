{{ 
    config(materialized='view') 
}}

with form_events as (
    select 
        * 
    from 
        {{ source('advanced_dbt_examples', 'form_events') }}
)

select 
    *
from form_events