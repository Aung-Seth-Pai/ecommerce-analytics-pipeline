-- Observed in the snapshot: every stock unit's cost equals its product's cost.
-- Returns the units where that stops being true.
{{ config(severity = 'warn') }}

select
    i.inventory_item_id,
    i.unit_cost,
    p.product_cost
from {{ ref('int_inventory_items_current') }} as i
join {{ ref('int_products_current') }} as p using (product_id)
where i.unit_cost != p.product_cost