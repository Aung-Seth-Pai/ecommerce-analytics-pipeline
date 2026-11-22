-- Staging rounds prices to cents and costs to 4 decimals. That only removes floating-point
-- error while the source sends no finer amounts. Returns the raw amounts the rounding would change.
{{ config(severity = 'warn') }}

with amounts as (

    select 'order_items.sale_price' as amount, id, sale_price as value, 2 as decimals
    from {{ source('ecommerce', 'order_items') }}
    union all
    select 'products.retail_price', id, retail_price, 2 from {{ source('ecommerce', 'products') }}
    union all
    select 'products.cost', id, cost, 4 from {{ source('ecommerce', 'products') }}
    union all
    select 'inventory_items.product_retail_price', id, product_retail_price, 2
    from {{ source('ecommerce', 'inventory_items') }}
    union all
    select 'inventory_items.cost', id, cost, 4 from {{ source('ecommerce', 'inventory_items') }}

)

select *
from amounts
where (decimals = 2 and abs(value - round(value, 2)) > 0.00005)
   or (decimals = 4 and abs(value - round(value, 4)) > 0.00005)