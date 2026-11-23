-- An order is finished (Complete, Returned or Cancelled) only when none of its items is still
-- open (Processing or Shipped). Returns the open items of finished orders. A Returned order may
-- keep Complete items: that is a partial return. Deleted orders and items are left out.

select
    orders.order_id,
    orders.order_status,
    order_items.order_item_id,
    order_items.order_item_status
from {{ ref('int_orders_current') }} as orders
join {{ ref('int_order_items_current') }} as order_items
    on orders.order_id = order_items.order_id
where not orders.is_deleted
  and not order_items.is_deleted
  and orders.order_status in ('Complete', 'Returned', 'Cancelled')
  and order_items.order_item_status in ('Processing', 'Shipped')