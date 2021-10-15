{{ config(materialized='view') }}

select 
    date_trunc(first_order_at, month) as first_order_month, 
    count(distinct customer_id) as customer_count
from {{ ref('customers') }}
group by 1
order by 1