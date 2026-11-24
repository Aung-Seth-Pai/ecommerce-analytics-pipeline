-- One row per browsing session, built from its events. A session's row can change until its
-- last event has arrived: one that crosses UTC midnight gets its later events in the next day's
-- batch. Browser, traffic source and IP location are the same for every event of a session
-- (tested in staging), so max() simply picks that value.

with events as (
    select * from {{ ref('stg_events') }}
),

sessions as (
    select
        session_id,
        max(user_id)                                                as user_id,
        max(browser)                                                as browser,
        max(session_traffic_source)                                 as session_traffic_source,
        max(ip_city)                                                as ip_city,
        max(ip_state)                                               as ip_state,
        max(ip_postal_code)                                         as ip_postal_code,
        min(created_at)                                             as session_started_at,
        max(created_at)                                             as session_ended_at,
        max(sequence_number)                                        as last_sequence_number,
        count(*)                                                    as event_count,
        count(case when event_type = 'home' then 1 end)             as home_event_count,
        count(case when event_type = 'department' then 1 end)       as department_event_count,
        count(case when event_type = 'product' then 1 end)          as product_event_count,
        count(case when event_type = 'cart' then 1 end)             as cart_event_count,
        count(case when event_type = 'purchase' then 1 end)         as purchase_event_count,
        count(case when event_type = 'cancel' then 1 end)           as cancel_event_count
    from events
    group by session_id
),

numbered as (
    select
        *,
        case
            when user_id is not null
            then row_number() over (partition by user_id order by session_started_at, session_id)
        end as user_session_number
    from sessions
)

select
    -- keys
    numbered.session_id,
    numbered.user_id,
    numbered.user_session_number,

    -- session attributes
    numbered.browser,
    numbered.session_traffic_source,
    numbered.ip_city,
    numbered.ip_state,
    numbered.ip_postal_code,

    -- time
    date(numbered.session_started_at)                               as session_date,
    numbered.session_started_at,
    numbered.session_ended_at,
    timestamp_diff(numbered.session_ended_at, numbered.session_started_at, second)
                                                                    as session_duration_seconds,

    -- path
    landing.event_type                                              as landing_event_type,
    landing.uri                                                     as landing_uri,
    exit_event.event_type                                           as exit_event_type,
    exit_event.uri                                                  as exit_uri,

    -- measures
    numbered.event_count,
    numbered.home_event_count,
    numbered.department_event_count,
    numbered.product_event_count,
    numbered.cart_event_count,
    numbered.purchase_event_count,
    numbered.cancel_event_count

from numbered
left join events as landing
    on numbered.session_id = landing.session_id
   and landing.sequence_number = 1
left join events as exit_event
    on numbered.session_id = exit_event.session_id
   and exit_event.sequence_number = numbered.last_sequence_number