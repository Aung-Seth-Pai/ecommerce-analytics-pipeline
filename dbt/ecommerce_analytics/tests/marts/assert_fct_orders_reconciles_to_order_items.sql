-- For each order, its item count and gross and net sales must equal the sums of its rows in
-- fct_order_items. Returns the orders where they don't, from either side.

with from_items as (
    select
        order_id,
        count(*)                as item_count,
        count(case when net_sales > 0 then 1 end)  as kept_item_count,
        sum(sale_price)         as gmv,
        sum(net_sales)          as net_sales,
        sum(gross_profit)       as gross_profit
    from {{ ref('fct_order_items') }}
    group by order_id
),

from_orders as (
    select order_id, item_count, kept_item_count, gmv, net_sales, gross_profit
    from {{ ref('fct_orders') }}
)

select
    coalesce(o.order_id, i.order_id)    as order_id,
    o.item_count                        as order_item_count,
    i.item_count                        as summed_item_count,
    o.kept_item_count                   as order_kept_item_count,
    i.kept_item_count                   as summed_kept_item_count,
    o.gmv                               as order_gmv,
    i.gmv                               as summed_gmv,
    o.net_sales                         as order_net_sales,
    i.net_sales                         as summed_net_sales,
    o.gross_profit                      as order_gross_profit,
    i.gross_profit                      as summed_gross_profit
from from_orders as o
full outer join from_items as i
    on o.order_id = i.order_id
where coalesce(o.item_count, 0) != coalesce(i.item_count, 0)
   or coalesce(o.gmv, 0) != coalesce(i.gmv, 0)
   or coalesce(o.net_sales, 0) != coalesce(i.net_sales, 0)
   or coalesce(o.gross_profit, 0) != coalesce(i.gross_profit, 0)
   or coalesce(o.kept_item_count, 0) != coalesce(i.kept_item_count, 0)