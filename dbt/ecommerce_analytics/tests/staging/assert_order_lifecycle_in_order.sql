-- Orders and order items move forward in time: created, then shipped, then delivered, then returned.
-- Item created_at is synthetic in thelook, so items are checked from shipped_at on.
-- Returns the versions where a later milestone comes before an earlier one.

select 'order' as entity, order_id as id, _batch_id, created_at, shipped_at, delivered_at, returned_at
from {{ ref('stg_orders') }}
where shipped_at < created_at
   or delivered_at < shipped_at
   or returned_at < delivered_at

union all

select 'order item', order_item_id, _batch_id, null, shipped_at, delivered_at, returned_at
from {{ ref('stg_order_items') }}
where delivered_at < shipped_at
   or returned_at < delivered_at