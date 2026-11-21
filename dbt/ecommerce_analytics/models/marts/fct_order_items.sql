-- One row per order item that is not deleted.
-- Revenue counts an item only while it is neither cancelled nor returned (current state):
-- a later return lowers the revenue of the item's original order date. For revenue by the
-- period in which money moved, use fct_sales_movements.

with order_items as (

    select * from {{ ref('int_order_items_joined') }}
    where not is_deleted

),

classified as (

    select
        *,
        order_item_status not in ('Cancelled', 'Returned') as counts_as_revenue
    from order_items

)

select
    -- keys
    order_item_id,
    order_id,                                       -- degenerate dimension
    user_id,
    product_id,
    distribution_center_id,

    -- dates (UTC), keys to dim_date
    date(order_created_at)                          as order_date,
    date(shipped_at)                                as shipped_date,
    date(delivered_at)                              as delivered_date,
    date(returned_at)                               as returned_date,

    -- timestamps (UTC)
    order_created_at,
    shipped_at,
    delivered_at,
    returned_at,

    -- status
    order_item_status,
    order_item_status = 'Cancelled'                 as is_cancelled,
    order_item_status = 'Returned'                  as is_returned,

    -- measures
    sale_price,
    unit_cost,
    sale_price - unit_cost                          as gross_margin,
    if(counts_as_revenue, sale_price, 0)            as net_sale_price,
    if(counts_as_revenue, sale_price - unit_cost, 0) as net_margin

from classified