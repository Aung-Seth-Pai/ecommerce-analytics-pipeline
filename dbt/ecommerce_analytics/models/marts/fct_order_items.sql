-- One row per order item that is not deleted. sale_price is what was ordered; net_sales counts
-- an item only while it is neither cancelled nor returned (current state), so a later return
-- lowers the net sales of the item's original order date. gross_profit is net sales minus the
-- cost of the kept items. For sales by the period in which money moved, use fct_sales_movements.

with order_items as (

    select * from {{ ref('int_order_items_joined') }}
    where not is_deleted

),

classified as (

    select
        *,
        order_item_status not in ('Cancelled', 'Returned') as counts_as_revenue
    from order_items

),

order_statuses as (

    select status, status_order
    from {{ ref('order_statuses') }}

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
    order_statuses.status_order                     as order_item_status_order,

    -- measures
    sale_price,
    unit_cost,
    case when counts_as_revenue then sale_price else 0 end              as net_sales,
    case when counts_as_revenue then sale_price - unit_cost else 0 end  as gross_profit,
    shipping_distance_km

from classified
left join order_statuses
    on classified.order_item_status = order_statuses.status