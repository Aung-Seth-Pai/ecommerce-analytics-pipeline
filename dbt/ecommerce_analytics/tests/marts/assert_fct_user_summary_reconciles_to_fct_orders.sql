-- For each user, the order history in fct_user_summary must equal what their rows in fct_orders
-- add up to. Returns the users where it doesn't, from either side.

with from_orders as (
    select
        user_id,
        count(*)                                                    as order_count,
        count(case when order_status = 'Cancelled' then 1 end)      as cancelled_order_count,
        sum(net_sales)                                              as lifetime_net_sales
    from {{ ref('fct_orders') }}
    group by user_id
),

from_summary as (
    select user_id, order_count, cancelled_order_count, lifetime_net_sales
    from {{ ref('fct_user_summary') }}
)

select
    coalesce(s.user_id, o.user_id)          as user_id,
    s.order_count                           as summary_order_count,
    o.order_count                           as fct_order_count,
    s.cancelled_order_count                 as summary_cancelled_order_count,
    o.cancelled_order_count                 as fct_cancelled_order_count,
    s.lifetime_net_sales                    as summary_lifetime_net_sales,
    o.lifetime_net_sales                    as fct_lifetime_net_sales
from from_summary as s
full outer join from_orders as o
    on s.user_id = o.user_id
where coalesce(s.order_count, 0) != coalesce(o.order_count, 0)
   or coalesce(s.cancelled_order_count, 0) != coalesce(o.cancelled_order_count, 0)
   or coalesce(s.lifetime_net_sales, 0) != coalesce(o.lifetime_net_sales, 0)