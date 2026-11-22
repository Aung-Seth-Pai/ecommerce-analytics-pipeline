-- Every event of a session has the same user, browser, traffic source and IP address.
-- int_users_web_activity takes one value of each per session and relies on this.
-- Returns the sessions where any of them varies.

select
    session_id,
    count(distinct user_id)                as users,
    count(distinct browser)                as browsers,
    count(distinct session_traffic_source) as traffic_sources,
    count(distinct ip_address)             as ip_addresses
from {{ ref('stg_events') }}
group by session_id
having count(distinct user_id) > 1
    or count(distinct browser) > 1
    or count(distinct session_traffic_source) > 1
    or count(distinct ip_address) > 1