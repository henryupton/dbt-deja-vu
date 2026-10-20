{{
    config(
        materialized='guarded_overwrite',
        tags=['fp_prune']
    )
}}

{# The current roster, read through the view; never prunable, whatever the dim did. #}
select
    agent_id,
    agent_name,
    team,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_dim_agent_current') }}
