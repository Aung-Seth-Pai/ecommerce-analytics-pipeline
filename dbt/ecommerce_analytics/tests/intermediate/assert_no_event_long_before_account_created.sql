-- Events may be linked to a user from shortly before their account was created (browsing,
-- then signing up), but not from more than a day before.
-- Returns the events dated more than 24 hours before their user's account was created.

select
    events.event_id,
    events.user_id,
    events.created_at  as event_created_at,
    users.created_at   as user_created_at
from {{ ref('stg_events') }} as events
join {{ ref('int_users_current') }} as users using (user_id)
where events.created_at < timestamp_sub(users.created_at, interval 24 hour)