{#
  Test-only helpers for the fp_prune group, which builds one EDP-shaped line twice from the same frozen inputs: a
  control arm with the guard off and a treatment arm with it on, each in its own schema with its own ledger.

  Schemas: models configured with schema 'prune_arm' land in <target schema>_prune_<fp_prune_arm var>, the frozen
  inputs (schema 'prune_input') in <target schema>_prune_input. Every other node resolves as dbt's default does.
#}
{% macro generate_schema_name(custom_schema_name, node) %}
  {%- if custom_schema_name is none -%}
    {{ return(target.schema) }}
  {%- elif (custom_schema_name | trim) == 'prune_arm' -%}
    {{ return(fp_prune_schema(var('fp_prune_arm', 'control'))) }}
  {%- endif -%}
  {{ return(target.schema ~ '_' ~ (custom_schema_name | trim)) }}
{% endmacro %}

{% macro fp_prune_schema(arm) %}
  {{ return(target.schema ~ '_prune_' ~ arm) }}
{% endmacro %}

{#
  dbt run-operation fp_prune_setup: creates the group's schemas with one day of Time Travel (a dev database may
  default to 0, and the fingerprint reads the pre-build table through it), so every table inherits it. Logs each
  schema for the runner, which passes the arm's as fingerprint_schema.
#}
{% macro fp_prune_setup() %}
  {%- for part in ['input', 'control', 'treatment'] -%}
    {%- set schema = target.database ~ '.' ~ fp_prune_schema(part) -%}
    {%- do run_query("create schema if not exists " ~ schema ~ " data_retention_time_in_days = 1") -%}
    {%- do run_query("alter schema " ~ schema ~ " set data_retention_time_in_days = 1") -%}
    {%- do log("fp_prune: " ~ part ~ " schema " ~ fp_prune_schema(part), info=true) -%}
  {%- endfor -%}
{% endmacro %}

{#
  The audit columns EDP's audit_columns macro renders into every model: a run-start literal, which moves the
  compiled SQL every run until fingerprint_sql_mask_run_started_at names its template, and a write clock. Both are
  in the fingerprint's default exclusions, and the arms oracle leaves them out too.
#}
{% macro fp_prune_audit_columns() %}
  {{ return("'" ~ run_started_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ") ~ "'::timestamp_tz as _batched_at,\n    sysdate()::timestamp_tz as _written_at") }}
{% endmacro %}
