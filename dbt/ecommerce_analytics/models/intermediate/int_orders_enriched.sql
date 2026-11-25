-- One row per order from current state, deleted orders kept and flagged. Milestones are the
-- order's own timestamps; item timestamps are never rolled up. From its non-deleted items the
-- order takes counts, money and the number of distribution centers it ships from.

with orders as (
    select * from {{ ref('int_orders_current') }}
),

order_items as (
    select * from {{ ref('int_order_items_joined') }}
    where not is_deleted
),

cutoff as (
    select data_cutoff_at from {{ ref('int_data_cutoff') }}
),

items_per_order as (
    select
        order_id,
        count(*)                                                        as item_count,
        count(case when order_item_status = 'Cancelled' then 1 end)     as cancelled_item_count,
        count(case when order_item_status = 'Returned' then 1 end)      as returned_item_count,
        count(case when order_item_status not in ('Cancelled', 'Returned')
                   then 1 end)                                          as kept_item_count,
        count(distinct order_item_status) > 1                           as has_mixed_item_status,
        count(distinct distribution_center_id)                          as distribution_center_count,
        sum(sale_price)                                                 as gmv,
        sum(case when order_item_status not in ('Cancelled', 'Returned')
                 then sale_price else 0 end)                            as net_sales,
        sum(case when order_item_status not in ('Cancelled', 'Returned')
                 then sale_price - unit_cost else 0 end)                as gross_profit
    from order_items
    group by order_id
),

numbered as (
    select
        *,
        case
            when not is_deleted
            then row_number() over (partition by user_id, is_deleted order by created_at, order_id)
        end as user_order_number
    from orders
)

select
    -- ids
    numbered.order_id,
    numbered.user_id,

    -- status and sequence
    numbered.order_status,
    numbered.user_order_number,

    -- milestones (the order's own)
    numbered.created_at                             as order_created_at,
    numbered.shipped_at,
    numbered.delivered_at,
    numbered.returned_at,

    -- maturity against the data cutoff
    coalesce(
        numbered.delivered_at <= cutoff.data_cutoff_at - interval {{ var('return_window_days') }} day,
        false
    )                                               as is_return_window_closed,
    coalesce(
        numbered.returned_at <= numbered.delivered_at + interval {{ var('return_window_days') }} day,
        false
    )                                               as is_returned_within_window,
    numbered.created_at <= cutoff.data_cutoff_at - interval {{ var('delivery_window_days') }} day
                                                    as is_delivery_window_closed,


    -- item mix
    coalesce(items.item_count, 0)                   as item_count,
    coalesce(items.cancelled_item_count, 0)         as cancelled_item_count,
    coalesce(items.returned_item_count, 0)          as returned_item_count,
    coalesce(items.kept_item_count, 0)              as kept_item_count,
    coalesce(items.has_mixed_item_status, false)    as has_mixed_item_status,
    coalesce(items.distribution_center_count, 0)    as distribution_center_count,

    -- money
    coalesce(items.gmv, 0)                          as gmv,
    coalesce(items.net_sales, 0)                    as net_sales,
    coalesce(items.gross_profit, 0)                 as gross_profit,

    -- metadata
    numbered.is_deleted

from numbered
left join items_per_order as items
    on numbered.order_id = items.order_id
cross join cutoff