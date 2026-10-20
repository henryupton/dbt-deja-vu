{{
    config(
        materialized='guarded_overwrite',
        tags=['fp_prune']
    )
}}

{# The unaffected sibling: whatever the agent line does, this prunes. #}
select
    country_code,
    country_name,
    region,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_dim_country') }}
where _is_current
