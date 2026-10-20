{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='report_id',
        tags=['fp_prune']
    )
}}

{# The built-in incremental: create or replace under --full-refresh (new, which blocks its child), otherwise #}
{# delete+insert of the rows above its own watermark, which writes nothing when no report landed. occurred_at is #}
{# the as-of cursor, cast as EDP's event casts it. #}
select
    report_id,
    agent_id,
    report_date,
    report_date::timestamp_tz as occurred_at,
    scheduled_minutes,
    adherent_minutes,
    _loaded_at,
    {{ fp_prune_audit_columns() }}
from {{ ref('fp_prune_stg_reports') }}
{% if is_incremental() %}
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
