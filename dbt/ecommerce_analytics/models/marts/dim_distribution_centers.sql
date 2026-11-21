-- One row per distribution center (current state).

with distribution_centers as (

    select * from {{ ref('int_distribution_centers_current') }}

)

select
    -- key
    distribution_center_id,

    -- attributes
    distribution_center_name,
    latitude,
    longitude,
    distribution_center_location,

    -- flags
    is_deleted

from distribution_centers