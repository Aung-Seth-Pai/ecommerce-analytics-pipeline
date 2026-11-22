-- A user cannot place an order before their account exists.
-- Returns the orders placed before their user's account was created.

select
    orders.order_id,
    orders.user_id,
    orders.created_at  as order_created_at,
    users.created_at   as user_created_at
from {{ ref('int_orders_current') }} as orders
join {{ ref('int_users_current') }} as users using (user_id)
where orders.created_at < users.created_at