-- A status must agree with the milestones that have happened:
-- Processing: not shipped. Shipped: shipped, not delivered. Complete: delivered.
-- Returned: returned. Cancelled: not shipped.
-- Returns the order and order item versions where they disagree.

with versions as (

    select 'order' as entity, order_id as id, _batch_id, order_status as status,
           shipped_at, delivered_at, returned_at
    from {{ ref('stg_orders') }}

    union all

    select 'order item', order_item_id, _batch_id, order_item_status,
           shipped_at, delivered_at, returned_at
    from {{ ref('stg_order_items') }}

)

select *
from versions
where (status = 'Processing' and shipped_at is not null)
   or (status = 'Shipped'    and (shipped_at is null or delivered_at is not null))
   or (status = 'Complete'   and delivered_at is null)
   or (status = 'Returned'   and returned_at is null)
   or (status = 'Cancelled'  and shipped_at is not null)