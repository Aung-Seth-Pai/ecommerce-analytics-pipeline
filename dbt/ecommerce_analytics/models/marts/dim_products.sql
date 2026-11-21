-- One row per product (current state).

with products as (

    select * from {{ ref('int_products_current') }}

)

select
    -- key
    product_id,

    -- attributes
    product_name,
    brand,
    category,
    department,
    sku,

    -- prices
    retail_price,
    product_cost,

    -- flags
    is_deleted

from products