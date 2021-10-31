{{ config(materialized='table') }}

with base_customers as (
    select 
        *
    from {{ ref('stg_customers') }} 
),

orders_per_customer as (
    select  
        customer_id, 
        min(created_at) as first_order_at, 
        count(distinct id) as number_of_orders 
    from 
        {{ ref('stg_orders') }} 
    group by 1
),

customers_orders as (
    select 
        orders_per_customer.customer_id, 
        base_customers.name, 
        base_customers.email, 
        orders_per_customer.first_order_at, 
        orders_per_customer.number_of_orders
    from 
        orders_per_customer 
    left join base_customers on base_customers.id = orders_per_customer.customer_id
)

select 
    customer_id, 
    name, 
    email, 
    first_order_at, 
    number_of_orders
from customers_orders 
order by 4