-- Latest version of each order item: one row per order_item_id.
{{ current_state(ref('stg_order_items'), 'order_item_id') }}