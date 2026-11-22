-- One row per money movement on an order item: a sale booked when the order is placed,
-- and a full refund booked when the item is returned. Cancelled items were never charged,
-- so they have no rows.

with order_items as (
    select * from {{ ref('int_order_items_joined') }}
    where not is_deleted
),

sales as (
    select
        order_item_id,
        order_id,
        user_id,
        product_id,
        distribution_center_id,
        'sale' as movement_type,
        order_created_at as movement_at,
        sale_price as amount
    from order_items
    where order_item_status != 'Cancelled'
),

returns as (
    select
        order_item_id,
        order_id,
        user_id,
        product_id,
        distribution_center_id,
        'return' as movement_type,
        returned_at as movement_at,
        -sale_price as amount
    from order_items
    where order_item_status = 'Returned'
),

movements as (
    select * from sales
    union all
    select * from returns
)

select
    {{ dbt_utils.generate_surrogate_key(['order_item_id', 'movement_type']) }} as sales_movement_id,
    order_item_id,
    order_id,
    user_id,
    product_id,
    distribution_center_id,
    movement_type,
    date(movement_at) as movement_date,
    movement_at,
    amount
from movements