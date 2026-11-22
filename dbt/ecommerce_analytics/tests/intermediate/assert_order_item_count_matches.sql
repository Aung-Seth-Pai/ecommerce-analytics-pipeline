-- An order's item_count equals its number of current, non-deleted items, so no order is without items.
-- Returns the orders where the two differ.

with items_per_order as (

    select order_id, count(*) as items
    from {{ ref('int_order_items_current') }}
    where not is_deleted
    group by order_id

)

select
    orders.order_id,
    orders.item_count,
    coalesce(items_per_order.items, 0) as items
from {{ ref('int_orders_current') }} as orders
left join items_per_order using (order_id)
where not orders.is_deleted
  and orders.item_count is distinct from coalesce(items_per_order.items, 0)