{{
    config(
        materialized='guarded_overwrite',
        tags=['fp_prune']
    )
}}

{# A row-level mart over the fact, so rows appended to the fact are appended here too. #}
select
    report_id,
    agent_id,
    report_date,
    team,
    round(adherence_ratio * 100, 1) as adherence_pct,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_fct_daily') }}
