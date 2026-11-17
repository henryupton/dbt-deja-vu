{{
    config(
        materialized='guarded_table',
        tags=['fp_knob']
    )
}}

{# The knob group's control: a plain guarded child of an unchanged parent, so it skips after the initial. #}
select customer_id, email, status
from {{ ref('fp_knob_parent') }}
