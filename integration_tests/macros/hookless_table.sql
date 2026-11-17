{#
  Test-only materialization: a create or replace that runs no hooks, the shape of the serving-layer types
  (streamlit, semantic_view, agent, mcp_server, cortex_search) whose materializations never call run_hooks.
  Its fingerprint_post never runs, so a registration would sit pending for good; the suite lists it in
  fingerprint_exclude_materializations instead.
#}

{% materialization hookless_table, default %}

  {%- set target_relation = this.incorporate(type='table') -%}

  {%- call statement('main') -%}
    {{ get_create_table_as_sql(False, target_relation, sql) }}
  {%- endcall -%}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
