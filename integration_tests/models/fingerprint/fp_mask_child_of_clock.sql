{{
    config(
        materialized='guarded_table',
        tags=['fp_mask']
    )
}}

{# Renders the run start in the same format, but in logic, where it can move the output: the mask's template names #}
{# the stamp's alias, so this rendering stays in the SQL hash and the model builds every run. #}
select customer_id, status
from {{ ref('fp_mask_parent') }}
where _loaded_at <= '{{ run_started_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ") }}'::timestamp_tz
