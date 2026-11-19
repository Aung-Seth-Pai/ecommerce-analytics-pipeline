with source as (

    select * from {{ source('ecommerce', 'users') }}
    where _batch_id in (select batch_id from {{ source('ecommerce', '_loaded_batches') }})

),

deduplicated as (

    -- drop reload copies: one row per id per batch, the latest load wins
    select * from source
    qualify row_number() over (partition by id, _batch_id order by _loaded_at desc, _seq desc) = 1

),

renamed as (

    select
        -- ids
        id as user_id,

        -- attributes
        first_name,
        last_name,
        email,
        age,
        gender,
        traffic_source as user_traffic_source,

        -- address
        street_address,
        postal_code,
        city,
        state,
        country,
        latitude,
        longitude,
        safe.st_geogfromtext(user_geom) as user_location,

        -- timestamps
        created_at,

        -- metadata
        _op,
        _source_updated_at,
        _seq,
        _batch_id,
        _batch_date,
        _loaded_at

    from deduplicated

)

select * from renamed