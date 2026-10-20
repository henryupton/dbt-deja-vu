{#
  Test-only: EDP's as_of_join shape, which fp_prune_fct_daily renders and no other model does. A left join onto the
  scd dim on a timestamp cursor inside each version's validity, falling back to the entity's first version for a
  row dated before it. The runner rewrites this file from scenario 93 with the same predicates the other way round,
  so the fact's compiled SQL moves while its output and its own checksum stay put.
#}
{% macro fp_prune_as_of_join(left, right, join_key, left_updated_at) %}
left join (
    select *, min(_valid_from) over (partition by {{ join_key }}) as _as_of_first_valid_from
    from {{ right }}
) as {{ right }}
    on {{ left }}.{{ join_key }} = {{ right }}.{{ join_key }}
    and (
        ({{ left }}.{{ left_updated_at }} >= {{ right }}._valid_from and {{ left }}.{{ left_updated_at }} < {{ right }}._valid_to)
        or ({{ left }}.{{ left_updated_at }} < {{ right }}._valid_from and {{ right }}._change_type = 'I'
            and {{ right }}._valid_from = {{ right }}._as_of_first_valid_from)
    )
{% endmacro %}
