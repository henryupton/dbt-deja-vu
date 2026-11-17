{{
    config(
        materialized='guarded_overwrite',
        tags=['fp_prune']
    )
}}

{# Business logic over the event: prunable only when the event is unchanged, which a --full-refresh never leaves it. #}
{# The runner adds a comment line here from scenario 102: a body-only change, which state:modified.body+ selects. #}
select
    report_id,
    agent_id,
    report_date,
    occurred_at,
    scheduled_minutes,
    adherent_minutes,
    adherent_minutes / nullif(scheduled_minutes, 0) as adherence_ratio,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_evt_report') }}
