{{
    config(
        materialized='guarded_table',
        tags=['fp_knob'],
        meta={
            'fingerprint_skip': var('fp_knob_wallclock_skip', true)
        }
    )
}}

{# Reads the wall clock, so its output moves while its parent stays put. The var flips the meta opt-out #}
{# without moving the checksum: skippable by default, opted out with fp_knob_wallclock_skip false. #}
select customer_id, status, current_date() as as_of_date
from {{ ref('fp_knob_parent') }}
