-- fct_order_items holds every non-deleted item of int_order_items_joined, with the same gross sales.
-- Returns a row when the item counts or the gross sales differ.

with fact as (

    select count(*) as items, sum(sale_price) as gross_sales
    from {{ ref('fct_order_items') }}

),

source as (

    select count(*) as items, sum(sale_price) as gross_sales
    from {{ ref('int_order_items_joined') }}
    where not is_deleted

)

select
    fact.items        as fact_items,
    source.items      as source_items,
    fact.gross_sales  as fact_gross_sales,
    source.gross_sales as source_gross_sales
from fact
cross join source
where fact.items != source.items
   or fact.gross_sales is distinct from source.gross_sales