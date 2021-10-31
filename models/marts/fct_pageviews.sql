with pageviews as (
    select 
        *
    from {{ ref('stg_pageviews') }}
    where customer_id is not null
),

one_visitor_id_per_customer as (
    select 
        customer_id, 
        min(visitor_id) as visitor_id
    from pageviews
    group by 1
),

final as (
    select 
        pageviews.id, 
        one_visitor_id_per_customer.visitor_id, 
        pageviews.device_type, 
        pageviews.timestamp, 
        pageviews.page, 
        pageviews.customer_id
    from pageviews
    left join one_visitor_id_per_customer   
        on pageviews.customer_id = one_visitor_id_per_customer.customer_id
    order by customer_id
)

select 
    *
from final