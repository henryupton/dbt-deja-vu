{{
    config(
        materialized='scd',
        unique_key=['country_code'],
        tags=['fp_prune'],
        meta={
            'scd_type': 2,
            'change_columns': {'exclude': ['_batched_at', '_written_at']},
            'full_refresh_strategy': var('fp_full_refresh_strategy', 'truncate')
        }
    )
}}

{# The sibling branch's scd dim: its input never changes, so after the baseline it reads unchanged in every deploy. #}
select
    country_code,
    country_name,
    region,
    _updated_at,
    _updated_at as _created_at,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_stg_country_lifecycle') }}
