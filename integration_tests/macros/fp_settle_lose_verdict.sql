{#
  Test-only hooks for the fp_settle fixtures: when fp_settle_lose names this node, the pre-hook renames the node
  ledger away and the post-hook, which runs after fingerprint_post, renames it back. The fingerprint post-hook in
  between can neither read the registration nor write a verdict, so the verdict is lost as a failed insert would
  lose it. A parked copy a killed run left behind stops the build: it is the whole ledger, so it is never dropped.

    pre_hook="{{ fp_settle_lose_verdict('park') }}", post_hook="{{ fp_settle_lose_verdict('restore') }}"
#}
{% macro fp_settle_lose_verdict(action) %}
  {%- if not execute or var('fp_settle_lose', none) != model.name -%}
    {{ return('select 1 where false') }}
  {%- endif -%}
  {%- set node = dbt_deja_vu.fingerprint_relation('deploy_node') -%}
  {%- set parked = api.Relation.create(database=node.database, schema=node.schema, identifier=node.identifier ~ '_parked', type='table') -%}
  {%- if action == 'park' -%}
    {%- set found = run_query("show tables like " ~ dbt_deja_vu.fingerprint_lit(parked.identifier | upper)
        ~ " in schema " ~ parked.database ~ "." ~ parked.schema) -%}
    {%- if found.rows | length > 0 -%}
      {{ exceptions.raise_compiler_error(parked ~ " exists: a killed run left the node ledger parked; rename it back to " ~ node) }}
    {%- endif -%}
    {{ return("alter table " ~ node ~ " rename to " ~ parked) }}
  {%- endif -%}
  {{ return("alter table " ~ parked ~ " rename to " ~ node) }}
{% endmacro %}
