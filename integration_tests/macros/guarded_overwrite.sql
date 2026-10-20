{#
  Test-only materialization: EDP's truncate_insert with the guard in it, for the fp_prune group. It keeps the
  object under --full-refresh: create or replace only when the relation is missing, is not a table, or the
  force_full_refresh var is set; otherwise insert overwrite by column name (fp_insert_overwrite_sql, shared with
  overwrite_table). The guard sits after that decision and before the pre-hooks, so a skip runs nothing, and a
  replace is never skipped: it is the build that makes a fresh object.
#}

{% materialization guarded_overwrite, default %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set do_replace = existing_relation is none or not existing_relation.is_table
      or (var('force_full_refresh', false) | string | lower) in ['true', '1'] -%}

  {%- if not do_replace and dbt_deja_vu.fingerprint_should_skip() -%}
    {%- do dbt_deja_vu.fingerprint_mark_skipped(detail='no blocking parent verdict') -%}
    {%- do log("guarded_overwrite: skipping " ~ target_relation ~ ", upstream unchanged", info=true) -%}
    {%- call statement('main') -%}
      select 'skipped' as outcome
    {%- endcall -%}
    {{ return({'relations': [existing_relation]}) }}
  {%- endif -%}

  {{ run_hooks(pre_hooks) }}

  {%- call statement('main') -%}
    {%- if do_replace -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- else -%}
      {{ fp_insert_overwrite_sql(target_relation, existing_relation, sql) }}
    {%- endif -%}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
