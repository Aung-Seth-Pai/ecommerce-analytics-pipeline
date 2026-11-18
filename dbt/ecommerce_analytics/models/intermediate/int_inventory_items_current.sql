-- Latest version of each inventory item: one row per inventory_item_id.
{{ current_state(ref('stg_inventory_items'), 'inventory_item_id') }}