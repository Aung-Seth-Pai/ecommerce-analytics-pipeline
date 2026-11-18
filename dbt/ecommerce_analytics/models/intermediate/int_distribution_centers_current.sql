-- Latest version of each distribution center: one row per distribution_center_id.
{{ current_state(ref('stg_distribution_centers'), 'distribution_center_id') }}