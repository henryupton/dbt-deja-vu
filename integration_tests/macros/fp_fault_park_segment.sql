{#
  Test-only hooks for fp_fault_parked: with fp_fault_park_segment set, the pre-hook renames the segment ledger
  table away and the post-hook, which runs after fingerprint_post, renames it back. The fingerprint post-hook in
  between finds no segment table when it hashes, which must record error and leave the node built. A copy a killed
  run left parked is dropped first. No-op otherwise.

    pre_hook="{{ fp_fault_park_segment('park') }}", post_hook="{{ fp_fault_park_segment('restore') }}"
#}
{% macro fp_fault_park_segment(action) %}
  {%- if not execute or not var('fp_fault_park_segment', false) -%}
    {{ return('select 1 where false') }}
  {%- endif -%}
  {%- set seg = dbt_deja_vu.fingerprint_relation('deploy_segment') -%}
  {%- set parked = api.Relation.create(database=seg.database, schema=seg.schema, identifier=seg.identifier ~ '_parked', type='table') -%}
  {%- if action == 'park' -%}
    {%- do run_query("drop table if exists " ~ parked) -%}
    {{ return("alter table " ~ seg ~ " rename to " ~ parked) }}
  {%- endif -%}
  {{ return("alter table " ~ parked ~ " rename to " ~ seg) }}
{% endmacro %}
