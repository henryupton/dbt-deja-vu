{{
    config(
        materialized='guarded_overwrite',
        tags=['fp_prune']
    )
}}

{# The fact: each report joined to the agent version in force on its day, through the as-of macro only it renders. #}
select
    a.report_id,
    a.agent_id,
    a.report_date,
    d.team,
    a.scheduled_minutes,
    a.adherent_minutes,
    a.adherence_ratio,
    a._loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_int_adh') }} a
join {{ ref('fp_prune_dim_agent') }} d
    on d.agent_id = a.agent_id
    and {{ fp_prune_as_of_join('a.report_date', 'd') }}
