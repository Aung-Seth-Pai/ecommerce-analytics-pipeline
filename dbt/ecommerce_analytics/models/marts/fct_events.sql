-- One row per website event: a factless transaction fact, where counting rows is the measure.
-- Events are never updated. Browser, traffic source and IP location are the same for every
-- event of a session, so they live once on fct_sessions.

with events as (
    select * from {{ ref('stg_events') }}
)

select
    -- keys
    event_id,
    session_id,
    user_id,
    case
        when event_type = 'product'
        then cast(regexp_extract(uri, r'^/product/([0-9]+)$') as int64)
    end                         as product_id,

    -- event
    sequence_number,
    event_type,
    uri,

    -- time
    date(created_at)            as event_date,
    created_at                  as event_at

from events