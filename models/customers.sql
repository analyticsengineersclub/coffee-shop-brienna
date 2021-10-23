{{ config(materialized='table') }}

with customers as (
    select 
        * 
    from 
        {{ source('coffee_shop', 'customers') }}
),

orders as (
    select 
        *
    from 
        {{ source('coffee_shop', 'orders') }}
),

orders_per_customer as (
    select  
        customer_id, 
        min(created_at) as first_order_at, 
        count(distinct id) as number_of_orders 
    from 
        orders 
    group by 1
),

customers_orders as (
    select 
        orders_per_customer.customer_id, 
        customers.name, 
        customers.email, 
        orders_per_customer.first_order_at, 
        orders_per_customer.number_of_orders
    from 
        orders_per_customer 
    left join customers on customers.id = orders_per_customer.customer_id
)

select 
    customer_id, 
    name, 
    email, 
    first_order_at, 
    number_of_orders
from customers_orders 
order by 4