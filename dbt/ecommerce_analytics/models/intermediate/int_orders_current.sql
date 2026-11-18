-- Latest version of each order: one row per order_id.
{{ current_state(ref('stg_orders'), 'order_id') }}