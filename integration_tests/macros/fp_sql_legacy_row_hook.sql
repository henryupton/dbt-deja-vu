{#
  Test-only pre-hook: when fp_sql_legacy_child names a node, append a copy of that node's latest build row with
  no sql_hash, as dbt-deja-vu 0.0.1 wrote it, stamped a millisecond after the original so it becomes the guard's
  baseline without postdating any parent verdict. No-op otherwise.

    pre_hook="{{ fp_sql_legacy_row_hook() }}"
#}
{% macro fp_sql_legacy_row_hook() %}
  {%- if not execute or var('fp_sql_legacy_child', none) is none -%}
    {{ return('select 1 where false') }}
  {%- endif -%}
  {%- set node_rel = dbt_deja_vu.fingerprint_relation('deploy_node') -%}
  {{ return("insert into " ~ node_rel
      ~ " (deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column, verdict, detail,"
      ~ " registered_at, finished_at, replaced, pre_rows, post_rows, watermark, appended_rows, touched_rows,"
      ~ " touched_old_rows, buckets_hashed, buckets_changed, pre_shape, post_shape, checksum)"
      ~ " select deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column, verdict, detail,"
      ~ " registered_at, dateadd('millisecond', 1, finished_at), replaced, pre_rows, post_rows, watermark, appended_rows,"
      ~ " touched_rows, touched_old_rows, buckets_hashed, buckets_changed, pre_shape, post_shape, checksum"
      ~ " from " ~ node_rel
      ~ " where node_name = " ~ dbt_deja_vu.fingerprint_lit(var('fp_sql_legacy_child')) ~ " and verdict <> 'pending'"
      ~ " qualify row_number() over (order by finished_at desc nulls last, registered_at desc) = 1") }}
{% endmacro %}
