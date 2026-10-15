{{
    config(
        enabled=(var('fingerprint', false) | string | lower) in ['true', '1'],
        tags=['fp_fault']
    )
}}

-- depends_on: {{ ref('fp_ledger') }}

{# With fp_fault_park_segment set, fp_fault_parked's post-hook loses the segment table as it hashes: its verdict #}
{# must be error, naming the failed step and carrying Snowflake's message. #}
with expected as (
    select 'fp_fault_parked' as node_name
    where {{ 'true' if var('fp_fault_park_segment', false) else 'false' }}
),
actual as (
    select node_name, verdict, detail
    from {{ dbt_deja_vu.fingerprint_relation('deploy_node') }}
    where deploy_id = '{{ var("deploy_id", invocation_id) }}'
    {{ dbt_deja_vu.fingerprint_latest_row() }}
)
select e.node_name, a.verdict, a.detail
from expected e
left join actual a on a.node_name = e.node_name
where a.node_name is null
   or not (a.verdict = 'error'
           and a.detail like 'fingerprint query failed (month hashes): 2003: %FINGERPRINT_DEPLOY_SEGMENT%')
