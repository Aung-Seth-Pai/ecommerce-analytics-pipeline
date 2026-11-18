{% macro current_state(relation, key, order_by="_source_updated_at desc, _batch_id desc, _seq desc") %}

    select
        *,
        _op = 'D' as is_deleted
    from {{ relation }}
    qualify row_number() over (partition by {{ key }} order by {{ order_by }}) = 1

{% endmacro %}