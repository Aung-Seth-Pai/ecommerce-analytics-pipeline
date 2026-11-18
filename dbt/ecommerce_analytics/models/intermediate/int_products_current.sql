-- Latest version of each product: one row per product_id.
{{ current_state(ref('stg_products'), 'product_id') }}