-- fct_order_items holds every non-deleted item of int_order_items_joined, with the same gross sales.
-- Returns a row when the item counts or the gross sales differ.

with fact as (

    select count(*) as items, sum(sale_price) as gmv
    from {{ ref('fct_order_items') }}

),

source as (

    select count(*) as items, sum(sale_price) as gmv
    from {{ ref('int_order_items_joined') }}
    where not is_deleted

)

select
    fact.items        as fact_items,
    source.items      as source_items,
    fact.gmv  as fact_gmv,
    source.gmv as source_gmv
from fact
cross join source
where fact.items != source.items
   or fact.gmv is distinct from source.gmv