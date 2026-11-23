-- One row per user: their order history and site activity as of the last build. An accumulating
-- fact at user grain: values change as the user keeps ordering and browsing. Users with no orders
-- or no events still get a row, with 0 counts and null dates.

with users as (
    select user_id from {{ ref('int_users_current') }}
),

order_history as (
    select * from {{ ref('int_users_order_history') }}
),

web_activity as (
    select * from {{ ref('int_users_web_activity') }}
)

select
    users.user_id,

    -- order history
    order_history.first_order_at,
    order_history.last_order_at,
    coalesce(order_history.order_count, 0)                  as order_count,
    coalesce(order_history.cancelled_order_count, 0)        as cancelled_order_count,
    coalesce(order_history.lifetime_net_sale_amount, 0)     as lifetime_net_sale_amount,

    -- site activity
    web_activity.first_seen_at,
    web_activity.last_seen_at,
    coalesce(web_activity.session_count, 0)                 as session_count,
    coalesce(web_activity.event_count, 0)                   as event_count,
    web_activity.primary_browser,
    web_activity.first_session_traffic_source,
    web_activity.last_session_traffic_source

from users
left join order_history
    on users.user_id = order_history.user_id
left join web_activity
    on users.user_id = web_activity.user_id