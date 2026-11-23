-- For each order, its item count and gross and net sales must equal the sums of its rows in
-- fct_order_items. Returns the orders where they don't, from either side.

with from_items as (
    select
        order_id,
        count(*)                as item_count,
        sum(sale_price)         as gross_sale_amount,
        sum(net_sale_price)     as net_sale_amount
    from {{ ref('fct_order_items') }}
    group by order_id
),

from_orders as (
    select order_id, item_count, gross_sale_amount, net_sale_amount
    from {{ ref('fct_orders') }}
)

select
    coalesce(o.order_id, i.order_id)    as order_id,
    o.item_count                        as order_item_count,
    i.item_count                        as summed_item_count,
    o.gross_sale_amount                 as order_gross_sale_amount,
    i.gross_sale_amount                 as summed_gross_sale_amount,
    o.net_sale_amount                   as order_net_sale_amount,
    i.net_sale_amount                   as summed_net_sale_amount
from from_orders as o
full outer join from_items as i
    on o.order_id = i.order_id
where coalesce(o.item_count, 0) != coalesce(i.item_count, 0)
   or coalesce(o.gross_sale_amount, 0) != coalesce(i.gross_sale_amount, 0)
   or coalesce(o.net_sale_amount, 0) != coalesce(i.net_sale_amount, 0)