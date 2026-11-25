-- For each order item, its movements must add up to its net_sales in fct_order_items.
-- Returns the items where they don't, from either side.

with movements as (
    select order_item_id, sum(amount) as movement_total
    from {{ ref('fct_sales_movements') }}
    group by order_item_id
),

order_items as (
    select order_item_id, net_sales
    from {{ ref('fct_order_items') }}
)

select
    coalesce(o.order_item_id, m.order_item_id) as order_item_id,
    o.net_sales,
    m.movement_total
from order_items as o
full outer join movements as m
    on o.order_item_id = m.order_item_id
where coalesce(o.net_sales, 0) != coalesce(m.movement_total, 0)