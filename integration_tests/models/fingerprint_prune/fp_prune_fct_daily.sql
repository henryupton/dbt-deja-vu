{{
    config(
        materialized='guarded_overwrite',
        tags=['fp_prune']
    )
}}

{# The fact: each report joined to the agent version in force when it occurred, through the as-of macro only it #}
{# renders (EDP's left join shape). A report whose agent has no version yet keeps a null team. #}
with adherence as (
    select * from {{ ref('fp_prune_int_adh') }}
),

dim_agent as (
    select * from {{ ref('fp_prune_dim_agent') }}
)

select
    adherence.report_id,
    adherence.agent_id,
    adherence.report_date,
    dim_agent.team,
    adherence.scheduled_minutes,
    adherence.adherent_minutes,
    adherence.adherence_ratio,
    adherence._loaded_at,
    {{ fp_prune_audit_columns() }}
from adherence
{{ fp_prune_as_of_join(left='adherence', right='dim_agent', join_key='agent_id', left_updated_at='occurred_at') }}
