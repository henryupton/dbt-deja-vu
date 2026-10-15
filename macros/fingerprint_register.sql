{#
  Content fingerprint: on-run-start registration.

  Registers only under a building command (flags.WHICH in the fingerprint_commands var):
  compile, docs generate, show and test fire on-run-start too, and a registration there would
  sit pending for good and block every child.

  Creates the ledger if missing (and adds sql_hash to a 0.0.1 ledger), then appends one `pending`
  row per selected model, seed or snapshot with the deploy id, its parents, its current column
  shape, its source checksum and `snapshot_at`, Snowflake's clock at run start. The pending row
  carries no sql_hash: nodes are not compiled yet, so the post-hook and the skip record it. The
  post-hook (fingerprint_post) reads the pre-build table through Time Travel at that anchor, so it
  is deliberately the database clock and not dbt's run_started_at. A node already registered for
  this deploy id (an earlier step of the same deploy) is left alone. Ephemeral models are not
  registered: they have no relation and run no hooks, so they could never leave `pending`; their
  descendants list the ephemeral's own parents instead. Nor are nodes whose materialization is in
  fingerprint_exclude_materializations (types that never run hooks); a child of one never skips.

  The insert goes in statements of at most fingerprint_register_batch_size rows (default 200):
  Snowflake cannot retry a statement over 1 MB, and each row carries the node's column shape.
  Every batch keeps the already-registered check, so a failure part way leaves the earlier
  batches registered and a retry under the same deploy id registers the rest.

  A failure never fails the hook, so it cannot skip a run (skip_nodes_if_on_run_start_fails):
  every statement goes through fingerprint_try_query, and the first one that fails logs a
  warning and stops registration with whatever committed. An unregistered node builds as usual,
  its post-hook is a no-op, and a guarded child built in the same run builds too, since a parent
  selected here with no registration cannot vouch for itself. Later runs of the deploy cannot see
  what this run built, as with the fingerprint off; the warning says so.

    on-run-start:
      - "{{ dbt_deja_vu.fingerprint_register() }}"

  Returns a no-op statement so the hook always has SQL to run.
#}

{#
  Logs why registration stopped and returns the hook's no-op. `committed` counts the insert statements that had
  already gone through.
#}
{% macro fingerprint_register_failed(deploy_id, step, error, committed=0) %}
  {%- do log("fingerprint: WARNING: registration for deploy " ~ deploy_id ~ " failed " ~ step ~ " (" ~ error ~ "); "
      ~ committed ~ " insert statement(s) had committed and nothing more is registered. Unregistered nodes build"
      ~ " unfingerprinted and their guarded children in this run build; later runs of this deploy cannot see what"
      ~ " this run built, so run them with fingerprint_skip_unchanged_upstream off", info=true) -%}
  {{ return('select 1 where false') }}
{% endmacro %}

{% macro fingerprint_register() %}
  {%- if not execute or not dbt_deja_vu.fingerprint_enabled() -%}
    {{ return('select 1 where false') }}
  {%- endif -%}
  {%- if not dbt_deja_vu.fingerprint_building_command() -%}
    {%- do log("fingerprint: " ~ dbt_deja_vu.fingerprint_command() ~ " is not in fingerprint_commands; nothing registered", info=true) -%}
    {{ return('select 1 where false') }}
  {%- endif -%}

  {%- set node_rel = dbt_deja_vu.fingerprint_relation('deploy_node') -%}
  {%- set seg_rel = dbt_deja_vu.fingerprint_relation('deploy_segment') -%}
  {%- set deploy_id = dbt_deja_vu.fingerprint_deploy_id() -%}
  {%- set ddl_error = dbt_deja_vu.fingerprint_ledger_ddl(node_rel, seg_rel) -%}
  {%- if ddl_error is not none -%}
    {{ return(dbt_deja_vu.fingerprint_register_failed(deploy_id, 'creating the ledger in ' ~ node_rel.database ~ '.' ~ node_rel.schema, ddl_error)) }}
  {%- endif -%}

  {%- set clock = dbt_deja_vu.fingerprint_try_query("select to_char(current_timestamp()::timestamp_tz, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')") -%}
  {%- if not clock.ok -%}
    {{ return(dbt_deja_vu.fingerprint_register_failed(deploy_id, 'reading the snapshot clock', clock.error)) }}
  {%- endif -%}
  {%- set snapshot_at = clock.table.columns[0].values()[0] -%}

  {%- set selected = dbt_deja_vu.fingerprint_selected_ids() -%}

  {# Collect the nodes, grouped by schema so the shape capture is one query per schema. #}
  {%- set items = [] -%}
  {%- set groups = {} -%}
  {%- for uid in selected -%}
    {%- set n = graph.nodes.get(uid) -%}
    {%- if n is not none and n.resource_type in ['model', 'seed', 'snapshot'] and not dbt_deja_vu.fingerprint_is_ephemeral(n)
        and not dbt_deja_vu.fingerprint_materialization_excluded(n) -%}
      {%- set alias = n.alias if (n.alias is defined and n.alias) else n.name -%}
      {%- set group_key = (n.database ~ '.' ~ n.schema) | upper -%}
      {%- if group_key not in groups -%}
        {%- do groups.update({group_key: {'database': n.database, 'schema': n.schema, 'tables': []}}) -%}
      {%- endif -%}
      {%- do groups[group_key]['tables'].append(alias | upper) -%}
      {%- do items.append({'uid': uid, 'node': n, 'alias': alias, 'group_key': group_key}) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- if items | length == 0 -%}
    {%- do log("fingerprint: nothing selected to register for deploy " ~ deploy_id, info=true) -%}
    {{ return('select 1 where false') }}
  {%- endif -%}

  {#
    One metadata-only SHOW per schema (dbt creates the schemas before on-run-start). Accounts that still cap SHOW output
    return exactly 10,000 rows; that one count falls back to per table, where a table not built yet (002003, does not
    exist) keeps a null pre_shape and the post-hook reads new.
  #}
  {%- set shapes = {} -%}
  {%- for group_key, g in groups.items() -%}
    {%- set res = dbt_deja_vu.fingerprint_try_query("show columns in schema " ~ g.database ~ "." ~ g.schema) -%}
    {%- if not res.ok -%}
      {{ return(dbt_deja_vu.fingerprint_register_failed(deploy_id, 'reading column shapes in ' ~ g.database ~ '.' ~ g.schema, res.error)) }}
    {%- elif res.table.rows | length != 10000 -%}
      {%- for t, shape in dbt_deja_vu.fingerprint_shape_from_show(res.table).items() -%}
        {%- do shapes.update({group_key ~ '.' ~ t: shape}) -%}
      {%- endfor -%}
    {%- else -%}
      {%- do log("fingerprint: show columns in " ~ g.database ~ "." ~ g.schema ~ " returned exactly 10,000 rows; capturing shapes per table", info=true) -%}
      {%- for t in g.tables | unique -%}
        {%- set one = dbt_deja_vu.fingerprint_try_query("show columns in table " ~ g.database ~ "." ~ g.schema ~ "." ~ t) -%}
        {%- if one.ok -%}
          {%- for tt, shape in dbt_deja_vu.fingerprint_shape_from_show(one.table).items() -%}
            {%- do shapes.update({group_key ~ '.' ~ tt: shape}) -%}
          {%- endfor -%}
        {%- elif one.code != '2003' -%}
          {{ return(dbt_deja_vu.fingerprint_register_failed(deploy_id, 'reading the column shape of ' ~ g.database ~ '.' ~ g.schema ~ '.' ~ t, one.error)) }}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set rows = [] -%}
  {%- for item in items -%}
    {%- set n = item.node -%}
    {%- set parents = [] -%}
    {%- for p in dbt_deja_vu.fingerprint_parent_ids(n) -%}
      {%- do parents.append(dbt_deja_vu.fingerprint_lit(p)) -%}
    {%- endfor -%}
    {%- do rows.append(
        "select " ~ dbt_deja_vu.fingerprint_lit(deploy_id) ~ " as deploy_id"
        ~ ", " ~ dbt_deja_vu.fingerprint_lit(item.uid) ~ " as node_id"
        ~ ", " ~ dbt_deja_vu.fingerprint_lit(n.name) ~ " as node_name"
        ~ ", " ~ dbt_deja_vu.fingerprint_lit(dbt_deja_vu.fingerprint_relation_name(n)) ~ " as relation_name"
        ~ ", array_construct(" ~ (parents | join(', ')) ~ ") as parents"
        ~ ", " ~ dbt_deja_vu.fingerprint_ts_lit(snapshot_at) ~ " as snapshot_at"
        ~ ", " ~ dbt_deja_vu.fingerprint_lit(dbt_deja_vu.fingerprint_loaded_at_column(n)) ~ " as loaded_at_column"
        ~ ", 'pending' as verdict, null as detail, current_timestamp() as registered_at"
        ~ ", " ~ dbt_deja_vu.fingerprint_lit(shapes.get(item.group_key ~ '.' ~ (item.alias | upper))) ~ " as pre_shape"
        ~ ", " ~ dbt_deja_vu.fingerprint_lit(dbt_deja_vu.fingerprint_node_checksum(n)) ~ " as checksum"
    ) -%}
  {%- endfor -%}

  {%- set batch_size = [var('fingerprint_register_batch_size', 200) | int, 1] | max -%}
  {%- set batches = [] -%}
  {%- for start in range(0, rows | length, batch_size) -%}
    {%- do batches.append(rows[start:start + batch_size]) -%}
  {%- endfor -%}
  {%- for batch in batches -%}
    {%- set ins = dbt_deja_vu.fingerprint_try_query(
        "insert into " ~ node_rel
        ~ " (deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column, verdict, detail, registered_at, pre_shape, checksum)"
        ~ " with r as (" ~ (batch | join(' union all ')) ~ ")"
        ~ " select r.deploy_id, r.node_id, r.node_name, r.relation_name, r.parents, r.snapshot_at, r.loaded_at_column, r.verdict, r.detail, r.registered_at, r.pre_shape, r.checksum"
        ~ " from r where not exists (select 1 from " ~ node_rel ~ " n where n.deploy_id = r.deploy_id and n.node_id = r.node_id)"
    ) -%}
    {%- if not ins.ok -%}
      {{ return(dbt_deja_vu.fingerprint_register_failed(deploy_id, 'inserting statement ' ~ loop.index ~ ' of ' ~ (batches | length), ins.error, committed=loop.index0)) }}
    {%- endif -%}
  {%- endfor -%}
  {%- do log("fingerprint: registered " ~ (rows | length) ~ " node(s) in " ~ (batches | length) ~ " statement(s) for deploy "
      ~ deploy_id ~ " at " ~ snapshot_at ~ " (command " ~ dbt_deja_vu.fingerprint_command() ~ ")", info=true) -%}

  {{ return('select 1 where false') }}
{% endmacro %}
