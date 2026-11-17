{{
    config(
        materialized='hookless_table',
        tags=['fp_knob']
    )
}}

{# Built by a materialization that runs no hooks and listed in fingerprint_exclude_materializations: never registered. #}
select customer_id, status
from {{ ref('fp_knob_parent') }}
