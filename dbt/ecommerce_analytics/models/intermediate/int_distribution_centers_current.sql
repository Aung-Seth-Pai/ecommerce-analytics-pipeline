-- Latest version of each order item: one row per order_item_id.
{{ current_state(ref('stg_distribution_centers'), 'distribution_center_id') }}