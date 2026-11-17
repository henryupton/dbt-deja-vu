{{
    config(
        materialized='guarded_table',
        tags=['fp_fault']
    )
}}

{# Two parents: fp_fault_parked's error verdict builds it, and so does fp_fault_parked built in a run that #}
{# registered nothing while fp_fault_parent is registered and unchanged. #}
select p.customer_id, p.email, k.status
from {{ ref('fp_fault_parent') }} p
join {{ ref('fp_fault_parked') }} k on p.customer_id = k.customer_id
