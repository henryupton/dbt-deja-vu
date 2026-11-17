{#
  Test-only materialization: EDP's truncate_insert with the guard in it, for the fp_prune group. It keeps the
  object under --full-refresh: create or replace only when the relation is missing, is not a table, or the
  force_full_refresh var is set; otherwise insert overwrite by column name (fp_insert_overwrite_sql, shared with
  overwrite_table). Hooks, docs, commit and grants run in truncate_insert's order. The guard sits after the replace
  decision and before the first pre-hook, so a skip runs none of them, and a replace is never skipped.
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

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- call statement('main') -%}
    {%- if do_replace -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- else -%}
      {{ fp_insert_overwrite_sql(target_relation, existing_relation, sql) }}
    {%- endif -%}
  {%- endcall -%}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do apply_grants(target_relation, config.get('grants', {}), should_revoke=true) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
