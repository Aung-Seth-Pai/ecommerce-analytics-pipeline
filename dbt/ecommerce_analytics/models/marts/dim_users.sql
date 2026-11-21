-- One row per user (current state), with their site activity. Personal data (names, email,
-- street address) is left out. Activity columns are overwritten as the user keeps browsing (Type 1).

with users as (

    select * from {{ ref('int_users_current') }}

),

web_activity as (
    
    select * from {{ ref('int_users_web_activity') }}

)

select
    -- key
    users.user_id,

    -- demographics
    users.age,
    case
        when users.age < 18 then 'Under 18'
        when users.age < 25 then '18-24'
        when users.age < 35 then '25-34'
        when users.age < 45 then '35-44'
        when users.age < 55 then '45-54'
        when users.age < 65 then '55-64'
        else '65+'
    end as age_band,
    case
        when users.age < 18 then 1
        when users.age < 25 then 2
        when users.age < 35 then 3
        when users.age < 45 then 4
        when users.age < 55 then 5
        when users.age < 65 then 6
        else 7
    end as age_band_order,
    users.gender,

    -- location
    users.country,
    users.state,
    users.city,
    users.postal_code,
    users.latitude,
    users.longitude,
    users.user_location,

    -- acquisition
    users.user_traffic_source,
    users.created_at as user_created_at,

    -- site activity (null or 0 for users with no events)
    web_activity.first_seen_at,
    web_activity.last_seen_at,
    coalesce(web_activity.session_count, 0) as session_count,
    coalesce(web_activity.event_count, 0)   as event_count,
    web_activity.primary_browser,
    web_activity.first_session_traffic_source,
    web_activity.last_session_traffic_source,

    -- flags
    users.is_deleted

from users
left join web_activity
    on web_activity.user_id = users.user_id