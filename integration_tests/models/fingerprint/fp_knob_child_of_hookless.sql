{{
    config(
        materialized='guarded_table',
        tags=['fp_knob']
    )
}}

{# Reads the unfingerprinted hookless table and the unchanged merge parent; the hookless side alone builds it. #}
select p.customer_id, p.email, h.status
from {{ ref('fp_knob_parent') }} p
join {{ ref('fp_knob_hookless') }} h on p.customer_id = h.customer_id
