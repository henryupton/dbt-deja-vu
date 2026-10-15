{{
    config(
        materialized='guarded_table',
        tags=['fp_mask']
    )
}}

{# Renders the run start as an audit stamp, the way an audit-columns macro does, so its compiled SQL moves every #}
{# run. _batched_at is left out of the content hash by default; the mask leaves it out of the SQL hash too. #}
select
    customer_id,
    status,
    '{{ run_started_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ") }}'::timestamp_tz as _batched_at
from {{ ref('fp_mask_parent') }}
