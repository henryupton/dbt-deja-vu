{{
    config(
        tags=['fp_prune_input']
    )
}}

{# Frozen staging: rebuilt by the runner between scenarios, never by a deploy, so both arms read the same rows. #}
select report_id, agent_id, report_date, scheduled_minutes, adherent_minutes, _loaded_at
from {{ ref('fp_prune_raw_reports') }}
where _stage <= {{ var('fp_prune_stage', 0) | int }}
