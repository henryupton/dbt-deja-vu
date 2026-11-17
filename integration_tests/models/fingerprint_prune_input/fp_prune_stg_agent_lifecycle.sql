{{
    config(
        tags=['fp_prune_input']
    )
}}

{# Frozen staging for the agent dim; fp_prune_stage decides which versions have landed. #}
select agent_id, agent_name, team, _updated_at, _loaded_at
from {{ ref('fp_prune_raw_agent_lifecycle') }}
where _stage <= {{ var('fp_prune_stage', 0) | int }}
