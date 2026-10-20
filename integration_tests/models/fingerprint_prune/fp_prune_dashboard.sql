{{
    config(
        materialized='hookless_table',
        tags=['fp_prune']
    )
}}

{# The streamlit stand-in: runs no hooks and is listed in fingerprint_exclude_materializations, so it is never #}
{# registered and builds in every deploy. #}
select
    country_code,
    region,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_dm_country') }}
