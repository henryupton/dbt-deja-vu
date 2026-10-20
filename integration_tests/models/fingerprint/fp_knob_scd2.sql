{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        tags=['fp_knob'],
        meta={
            'scd_type': 2,
            'full_refresh_strategy': var('fp_full_refresh_strategy', 'truncate')
        }
    )
}}

{# SCD type 2 on its own source var, so a new version of an old key (appended -> versioned) moves nothing #}
{# else in the group. Closing the old row moves _valid_to and _is_current, which the default hashes. #}
select
    customer_id,
    customer_name,
    email,
    status,
    _updated_at,
    _updated_at as _created_at,
    _loaded_at
from ({{ fp_customer_rows(var('fp_knob_scd_source', 'appended')) }})
