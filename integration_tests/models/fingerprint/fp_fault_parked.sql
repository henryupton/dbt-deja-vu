{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        pre_hook="{{ fp_fault_park_segment('park') }}",
        post_hook="{{ fp_fault_park_segment('restore') }}",
        tags=['fp_fault']
    )
}}

{# The merge rewrites every matched row, so its post-hook always hashes. With fp_fault_park_segment set, its #}
{# hooks take the segment ledger away for the length of that post-hook. #}
select customer_id, email, status, _loaded_at
from {{ ref('fp_fault_parent') }}
