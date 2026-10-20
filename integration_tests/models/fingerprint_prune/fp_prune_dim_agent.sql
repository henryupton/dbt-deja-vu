{{
    config(
        materialized='scd',
        unique_key=['agent_id'],
        tags=['fp_prune'],
        meta={
            'scd_type': 2,
            'change_columns': {'exclude': ['_batched_at', '_written_at']},
            'full_refresh_strategy': var('fp_full_refresh_strategy', 'truncate')
        }
    )
}}

{# The scd dim. Its materialization has no guard, so it always builds, and its verdict is what its children read. #}
select
    agent_id,
    agent_name,
    team,
    _updated_at,
    _updated_at as _created_at,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_stg_agent_lifecycle') }}
