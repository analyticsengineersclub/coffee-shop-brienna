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

pageviews_deduped as (
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
),

pageviews_lagging as (
    select 
        *,
        lag(timestamp, 1) over (partition by customer_id order by timestamp asc) as last_timestamp,
    from pageviews_deduped
),

pageviews_flagged as (
    select 
        *,
        if(date_diff(timestamp, last_timestamp, minute) <= 30, 0, 1) as is_new_session
    from pageviews_lagging
),

final as (
    select     
        id, 
        visitor_id, 
        device_type, 
        timestamp, 
        page, 
        customer_id,
        sum(is_new_session) over (order by customer_id, timestamp) as session_id
    from pageviews_flagged
)

select 
    *
from final