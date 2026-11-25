-- One row per user with at least one non-deleted order: their order history on current state.
-- Cancelled orders count as placed orders and are also counted on their own; revenue follows
-- the facts' rule. Overwritten as the user keeps ordering.

with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where not is_deleted
)

select
    user_id,
    min(order_created_at)                                       as first_order_at,
    max(order_created_at)                                       as last_order_at,
    count(*)                                                    as order_count,
    count(case when order_status = 'Cancelled' then 1 end)      as cancelled_order_count,
    sum(net_sales)                                              as lifetime_net_sales
from orders
group by user_id