-- One row per order that is not deleted: an accumulating snapshot. Each milestone has a date
-- (joins dim_date) and a timestamp; lags between milestones are in whole minutes and stay null
-- until the later milestone happens.

with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where not is_deleted
)

select
    -- keys
    order_id,
    user_id,

    -- status and sequence
    order_status,
    user_order_number,

    -- milestone dates
    date(order_created_at)                                  as order_date,
    date(shipped_at)                                        as shipped_date,
    date(delivered_at)                                      as delivered_date,
    date(returned_at)                                       as returned_date,

    -- milestone timestamps
    order_created_at,
    shipped_at,
    delivered_at,
    returned_at,

    -- lags
    timestamp_diff(shipped_at, order_created_at, minute)    as minutes_to_ship,
    timestamp_diff(delivered_at, shipped_at, minute)        as minutes_in_transit,
    timestamp_diff(delivered_at, order_created_at, minute)  as minutes_to_deliver,
    timestamp_diff(returned_at, delivered_at, minute)       as minutes_to_return,

    -- item mix
    item_count,
    cancelled_item_count,
    returned_item_count,
    has_mixed_item_status,
    distribution_center_count,

    -- measures
    gmv,
    net_sales,
    gross_profit

from orders