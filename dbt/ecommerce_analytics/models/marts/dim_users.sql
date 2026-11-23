-- One row per user (current state): descriptive attributes from the user record only. Personal
-- data is left out. Deleted users are kept, flagged by is_deleted. Order history and site
-- activity live in fct_user_summary.

with users as (
    select * from {{ ref('int_users_current') }}
)

select
    user_id,

    -- demographics
    age,
    case
        when age < 18 then 'Under 18'
        when age < 25 then '18-24'
        when age < 35 then '25-34'
        when age < 45 then '35-44'
        when age < 55 then '45-54'
        when age < 65 then '55-64'
        when age >= 65 then '65+'
    end as age_band,
    case
        when age < 18 then 1
        when age < 25 then 2
        when age < 35 then 3
        when age < 45 then 4
        when age < 55 then 5
        when age < 65 then 6
        when age >= 65 then 7
    end as age_band_order,
    gender,

    -- location
    country,
    state,
    city,
    postal_code,
    latitude,
    longitude,
    user_location,

    -- acquisition and account
    user_traffic_source,
    created_at as user_created_at,

    -- metadata
    is_deleted

from users