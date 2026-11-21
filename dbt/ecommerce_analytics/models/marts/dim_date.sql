-- One row per calendar day (UTC), from 2018-01-01 to 2028-12-31.
-- The facts' relationships tests to date_day fail when a date falls outside this range.

with days as (

    select date_day
    from unnest(generate_date_array('2018-01-01', '2028-12-31')) as date_day

)

select
    -- key
    date_day,

    -- year, quarter, month
    extract(year from date_day)                     as year,
    extract(quarter from date_day)                  as quarter,
    format_date('%Y-Q%Q', date_day)                 as year_quarter,
    extract(month from date_day)                    as month,
    format_date('%B', date_day)                     as month_name,
    format_date('%Y-%m', date_day)                  as year_month,

    -- week (ISO: weeks start on Monday)
    date_trunc(date_day, isoweek)                   as week_start_date,
    extract(isoweek from date_day)                  as iso_week,

    -- day
    extract(day from date_day)                      as day_of_month,
    extract(dayofyear from date_day)                as day_of_year,
    cast(format_date('%u', date_day) as int64)      as day_of_week,     -- 1 = Monday ... 7 = Sunday
    format_date('%A', date_day)                     as day_name,
    format_date('%u', date_day) in ('6', '7')       as is_weekend,
    date_day = last_day(date_day, month)            as is_last_day_of_month

from days