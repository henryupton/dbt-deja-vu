{{
    config(
        materialized='guarded_overwrite',
        tags=['fp_prune']
    )
}}

{# A mart copy of the dim with its validity columns, so a version that closes an old row has to rebuild it. #}
select
    agent_id,
    agent_name,
    team,
    _valid_from,
    _valid_to,
    _is_current,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_dim_agent') }}
