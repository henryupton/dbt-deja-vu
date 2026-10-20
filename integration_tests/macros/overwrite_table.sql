{#
  Test-only materialization: the insert-overwrite shape common to truncate-and-reload
  facts and marts. create or replace on the first build or --full-refresh, otherwise
  `insert overwrite into` the existing table by column name (quoted as stored, so mixed-case names
  survive) so the object, and its Time Travel, survive. Pre and post hooks run either way.
#}

{% materialization overwrite_table, default %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}

  {{ run_hooks(pre_hooks) }}

  {%- if existing_relation is none or not existing_relation.is_table or should_full_refresh() -%}
    {%- call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- endcall -%}
  {%- else -%}
    {%- call statement('main') -%}
      {{ fp_insert_overwrite_sql(target_relation, existing_relation, sql) }}
    {%- endcall -%}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{#
  The insert overwrite overwrite_table and guarded_overwrite share. The model SQL sits on its own line, so a
  trailing -- comment in it cannot swallow the closing bracket.
#}
{% macro fp_insert_overwrite_sql(target_relation, existing_relation, sql) %}
  {%- set quoted = [] -%}
  {%- for c in adapter.get_columns_in_relation(existing_relation) -%}{%- do quoted.append(adapter.quote(c.name)) -%}{%- endfor -%}
  {%- set cols_csv = quoted | join(', ') -%}
  {{ return("insert overwrite into " ~ target_relation ~ " (" ~ cols_csv ~ ")\nselect " ~ cols_csv ~ "\nfrom (\n" ~ sql ~ "\n)") }}
{% endmacro %}
