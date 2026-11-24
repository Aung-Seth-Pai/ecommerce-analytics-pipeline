-- Each session's event_count in fct_sessions must equal the number of its events in fct_events,
-- and every session must appear in both. Both are built from stg_events separately, so this
-- fails if a filter or change is ever applied to one and not the other.

with from_events as (
    select session_id, count(*) as event_count
    from {{ ref('fct_events') }}
    group by session_id
),

from_sessions as (
    select session_id, event_count
    from {{ ref('fct_sessions') }}
)

select
    coalesce(s.session_id, e.session_id)    as session_id,
    s.event_count                           as session_event_count,
    e.event_count                           as counted_event_count
from from_sessions as s
full outer join from_events as e
    on s.session_id = e.session_id
where coalesce(s.event_count, 0) != coalesce(e.event_count, 0)