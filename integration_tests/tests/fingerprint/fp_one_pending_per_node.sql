{{
    config(
        enabled=(var('fingerprint', false) | string | lower) in ['true', '1'],
        tags=['fingerprint', 'fp_overwrite', 'fp_evolve', 'fp_guard', 'fp_late', 'fp_edp', 'fp_edge', 'fp_knob', 'fp_sql', 'fp_fault', 'fp_settle', 'fp_mask', 'fp_prune']
    )
}}

-- depends_on: {{ ref('fp_ledger') }}

{# Registration is idempotent per deploy and node: a later step or a retry of the same deploy adds no second pending row. #}
select node_id, count_if(verdict = 'pending') as pending_rows, count(*) as ledger_rows
from {{ dbt_deja_vu.fingerprint_relation('deploy_node') }}
where deploy_id = '{{ var("deploy_id", invocation_id) }}'
group by node_id
having count_if(verdict = 'pending') <> 1
