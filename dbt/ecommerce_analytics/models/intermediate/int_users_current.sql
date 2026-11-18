-- Latest version of each user: one row per user_id.
{{ current_state(ref('stg_users'), 'user_id') }}