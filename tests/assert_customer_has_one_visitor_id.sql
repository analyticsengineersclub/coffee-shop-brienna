select
    customer_id,
    count(distinct visitor_id) as num_visitor_ids
from {{ ref('fct_pageviews' )}}
group by 1
having num_visitor_ids > 1