-- One row per user with at least one event: how they use the site, summarised from their sessions.
-- Built from stg_events (events are immutable: one row per event, unique event_id).
-- Browser and traffic source are the same for every event of a session, so a session has one of each.

with events as (

    select * from {{ ref('stg_events') }}
    where user_id is not null

),

sessions as (

    select
        user_id,
        session_id,
        min(created_at)                   as session_started_at,
        max(created_at)                   as session_ended_at,
        any_value(browser)                as browser,
        any_value(session_traffic_source) as session_traffic_source,
        count(*)                          as event_count
    from events
    group by user_id, session_id

),

browsers_ranked as (

    -- most sessions wins; on a tie, the browser of the most recent session
    select
        user_id,
        browser,
        row_number() over (
            partition by user_id
            order by count(*) desc, max(session_started_at) desc, browser
        ) as browser_rank
    from sessions
    group by user_id, browser

),

per_user as (

    select
        user_id,
        min(session_started_at) as first_seen_at,
        max(session_ended_at)   as last_seen_at,
        count(*)                as session_count,
        sum(event_count)        as event_count,
        array_agg(session_traffic_source ignore nulls order by session_started_at, session_id limit 1)[safe_offset(0)]
            as first_session_traffic_source,
        array_agg(session_traffic_source ignore nulls order by session_started_at desc, session_id desc limit 1)[safe_offset(0)]
            as last_session_traffic_source
    from sessions
    group by user_id

)

select
    per_user.user_id,
    per_user.first_seen_at,
    per_user.last_seen_at,
    per_user.session_count,
    per_user.event_count,
    browsers_ranked.browser as primary_browser,
    per_user.first_session_traffic_source,
    per_user.last_session_traffic_source
from per_user
join browsers_ranked
    on browsers_ranked.user_id = per_user.user_id
    and browsers_ranked.browser_rank = 1