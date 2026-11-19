with source as (

    select * from {{ source('ecommerce', 'inventory_items') }}
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
        id as inventory_item_id,
        product_id,
        product_distribution_center_id,

        -- attributes
        cost as unit_cost,
        product_name,
        product_brand,
        product_category,
        product_department,
        product_sku,
        product_retail_price,

        -- timestamps
        created_at,
        sold_at,

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