-- One row: the moment the loaded data runs up to. Each batch's _batch_date is the exclusive end
-- of its window, so the latest one across the tables that grow daily is the end of all loaded
-- data. Maturity rules and "data through" labels compare against this, never the current date,
-- so rebuilding the same data gives the same results.

with batch_dates as (
    select _batch_date from {{ ref('stg_orders') }}
    union all
    select _batch_date from {{ ref('stg_order_items') }}
    union all
    select _batch_date from {{ ref('stg_users') }}
    union all
    select _batch_date from {{ ref('stg_events') }}
)

select
    cast(max(_batch_date) as timestamp) as data_cutoff_at
from batch_dates