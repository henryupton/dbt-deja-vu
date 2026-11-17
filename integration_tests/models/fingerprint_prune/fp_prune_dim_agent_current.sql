{{
    config(
        materialized='view',
        tags=['fp_prune']
    )
}}

{# A view has no content of its own to fingerprint: it reads new on every build, so its child always builds. #}
select
    agent_id,
    agent_name,
    team,
    _valid_from,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_dim_agent') }}
where _is_current
