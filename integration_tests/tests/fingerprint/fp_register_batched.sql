{{
    config(
        enabled=(var('fingerprint', false) | string | lower) in ['true', '1'],
        tags=['fingerprint', 'fp_overwrite', 'fp_evolve', 'fp_guard', 'fp_late', 'fp_edp', 'fp_edge', 'fp_knob', 'fp_sql']
    )
}}

-- depends_on: {{ ref('fp_ledger') }}

{# Every row of one register statement shares its current_timestamp(), so no stamp may carry more pending rows than a batch. #}
select registered_at, count(*) as pending_rows
from {{ dbt_deja_vu.fingerprint_relation('deploy_node') }}
where deploy_id = '{{ var("deploy_id", invocation_id) }}'
  and verdict = 'pending'
group by registered_at
having count(*) > {{ [var('fingerprint_register_batch_size', 200) | int, 1] | max }}
