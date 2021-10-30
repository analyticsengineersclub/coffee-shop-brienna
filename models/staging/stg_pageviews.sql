{{ config(materialized='table') }}

with pageviews as (
    select 
        * 
    from 
        {{ source('web_tracking', 'pageviews') }}
)

select 
    *
from pageviews