{{
    config(
        materialized='guarded_table',
        tags=['fp_knob']
    )
}}

{# Reads validity the way an as-of join does, so a closed row has to rebuild it. #}
select customer_id, status, _valid_from, _valid_to
from {{ ref('fp_knob_scd2') }}
