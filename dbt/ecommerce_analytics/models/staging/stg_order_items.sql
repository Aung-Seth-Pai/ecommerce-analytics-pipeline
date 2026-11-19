with source as (

    select * from {{ source('ecommerce', 'order_items') }}
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
        id as order_item_id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,

        -- attributes
        status order_item_status,
        sale_price,

        -- timestamps
        created_at,
        shipped_at,
        delivered_at,
        returned_at,

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