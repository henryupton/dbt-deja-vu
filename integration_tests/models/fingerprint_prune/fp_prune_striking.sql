{{
    config(
        materialized='guarded_overwrite',
        tags=['fp_prune'],
        meta={
            'fingerprint_skip': false
        }
    )
}}

{# Reads the wall clock in logic, so its output can move while its parent stays put: opted out of pruning. Every #}
{# version took effect long before any run, so the rows themselves stay put. #}
select
    country_code,
    country_name,
    region,
    _valid_from,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_dim_country') }}
where _is_current
    and _valid_from::date <= current_date()
