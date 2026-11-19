-- One row per order item (current state), with its order's status and creation time
-- and its stock unit's cost and distribution center.

with order_items as (

    select * from {{ ref('int_order_items_current') }}

),

orders as (

    select * from {{ ref('int_orders_current') }}

),

inventory_items as (

    select * from {{ ref('int_inventory_items_current') }}

),

joined as (

    select
        -- ids
        order_items.order_item_id,
        order_items.order_id,
        order_items.user_id,
        order_items.product_id,
        order_items.inventory_item_id,
        inventory_items.product_distribution_center_id as distribution_center_id,

        -- attributes
        order_items.order_item_status,
        orders.order_status,

        -- measures
        order_items.sale_price,
        inventory_items.unit_cost,

        -- timestamps
        orders.created_at as order_created_at,
        order_items.shipped_at,
        order_items.delivered_at,
        order_items.returned_at,

        -- flags: a deleted order deletes its items too
        order_items.is_deleted or coalesce(orders.is_deleted, false) as is_deleted

    -- left joins keep an item whose parent is missing; the not_null tests then catch it
    from order_items
    left join orders
        on orders.order_id = order_items.order_id
    left join inventory_items
        on inventory_items.inventory_item_id = order_items.inventory_item_id

)

select * from joined