{#
  Content fingerprint: build guard.

  fingerprint_should_skip() answers "may this node's build be skipped?" from the ledger: true only
  when `fingerprint_skip_unchanged_upstream` is on, at least one parent is registered in this
  deploy, every registered parent finished `unchanged`, `appended` or `skipped`, and the node's
  own source checksum and compiled SQL hash match the ones recorded at its last fingerprinted
  build of this relation. A parent that is `pending`, `new`, `modified`, `unhashable` or `error`
  blocks. Ephemeral parents are looked through to their own parents. A node with no parents, none
  registered in this deploy, or no fingerprinted build of this relation on record is never
  skipped: the guard cannot tell why it is being built. Nor is a node whose own SQL changed,
  however its parents came out; the parents say nothing about that. Nor is a node, or a child of
  one, whose materialization is in fingerprint_exclude_materializations: it carries no verdict,
  so whether it was rebuilt is unknown. Nor is a node whose meta sets `fingerprint_skip: false`,
  for output that moves when its parents do not (a model reading the wall clock); it is still
  registered and fingerprinted, so its own children read its verdict as usual.

  The checksum is the node's own source file; the compiled SQL hash (fingerprint_sql_hash) also
  covers every macro it calls, var it renders and ephemeral it reads. A baseline with no hash
  (written by 0.0.1) never skips, nor does a node with no compiled SQL in context, nor SQL that
  renders a run-varying value (invocation_id, run start time), which is what a model rendering
  the clock needs. Config that changes how the materialization writes without changing the SQL
  (a unique_key set in YAML) moves neither; deploy it with `fingerprint_skip_unchanged_upstream`
  off.

  A parent's verdict is its latest real verdict in the deploy; a `skipped` row stands only when
  the parent recorded no verdict in that deploy, and a `reselected` row never (see
  fingerprint_latest_row).

  That verdict is measured from the deploy's snapshot, so it says nothing about a parent written
  between the node's last read and the snapshot, or rebuilt after the node earlier in the same
  deploy. The ledger since the node's last fingerprinted build or skip covers what it recorded;
  four records there build the node: a parent verdict whose snapshot predates that build (the
  parent was rebuilt after the node read it); a blocking verdict or a `pending` row for a parent in
  another deploy (a change the node never read); a parent registration from another run that no
  verdict or skip settled (a lost verdict, which an earlier step's verdict would otherwise hide);
  and a registration of the node under another checksum that recorded no verdict (a build whose
  hooks failed after the table was written). Registration precedes compilation, so it carries no
  compiled SQL hash to compare.

  Writes the ledger never saw, from a run with the fingerprint off, are invisible to the guard. A
  run that builds parents and their children together leaves them in step. After one that rebuilt
  a parent without its children, or built the node under other compiled SQL, deploy with
  `fingerprint_skip_unchanged_upstream` off.

  Every parent selected in this run must carry a verdict or skip this run recorded. One with no
  registration in this deploy was built where the ledger cannot see it: its registration failed
  under fingerprint_contain_register_failure, or the command is not in fingerprint_commands. One
  registered but unrecorded lost its verdict, and the verdict that counts is an earlier step's.
  Either way nothing vouches for this run's build, so the node builds.

  fingerprint_mark_skipped() appends the skip so the node's own children can read it. The skip row
  carries the node's compiled SQL hash too, since it is the baseline the node's next build reads.

  No statement either macro issues can fail the node; each goes through fingerprint_try_query. A
  guard query that fails logs a warning and answers false, so the node builds: an error never
  becomes a skip. A skip that cannot be recorded logs a warning and leaves the skip in place. The
  node's last row for the deploy still counts (pending when it recorded nothing), which is right
  because a skip writes nothing; but this run's registration stays unsettled, so its children
  build, now and at their next selection, and its own next build compares against the last row
  it did record.

  These are plain macros for a materialization to call; nothing in this package calls them.
#}

{% macro fingerprint_guard_failed(node, step, error) %}
  {%- do log("fingerprint: WARNING: " ~ node.name ~ " guard failed " ~ step ~ " (" ~ error ~ "); building", info=true) -%}
  {{ return(false) }}
{% endmacro %}

{% macro fingerprint_should_skip(node=none) %}
  {%- if not execute or not dbt_deja_vu.fingerprint_enabled() or not dbt_deja_vu.fingerprint_skip_enabled() -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set node = node if node is not none else model -%}
  {%- if dbt_deja_vu.fingerprint_materialization_excluded(node) -%}
    {%- do log("fingerprint: " ~ node.name ~ " is excluded from the fingerprint by materialization; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set opt_out = dbt_deja_vu.fingerprint_node_meta(node).get('fingerprint_skip') -%}
  {%- if opt_out is not none and (opt_out | string | lower) in ['false', '0'] -%}
    {%- do log("fingerprint: " ~ node.name ~ " sets meta fingerprint_skip false; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set parents = [] -%}
  {%- set unfingerprinted = [] -%}
  {# Parents built in this run: each must be registered in this deploy, or its build is invisible to the ledger. #}
  {%- set selected = dbt_deja_vu.fingerprint_selected_ids() -%}
  {%- set selected_parents = [] -%}
  {%- for p in dbt_deja_vu.fingerprint_parent_ids(node) -%}
    {%- do parents.append(dbt_deja_vu.fingerprint_lit(p)) -%}
    {%- if dbt_deja_vu.fingerprint_materialization_excluded(graph.nodes.get(p) if graph is defined else none) -%}
      {%- do unfingerprinted.append(p) -%}
    {%- endif -%}
    {%- if p in selected -%}{%- do selected_parents.append(dbt_deja_vu.fingerprint_lit(p)) -%}{%- endif -%}
  {%- endfor -%}
  {%- if parents | length == 0 -%}
    {{ return(false) }}
  {%- endif -%}
  {# A parent excluded by materialization carries no verdict, so nothing says it did not move. #}
  {%- if unfingerprinted | length > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " reads " ~ (unfingerprinted | join(', ')) ~ ", excluded from the fingerprint by materialization; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}

  {%- set node_rel = dbt_deja_vu.fingerprint_relation('deploy_node') -%}
  {%- set deploy_lit = dbt_deja_vu.fingerprint_lit(dbt_deja_vu.fingerprint_deploy_id()) -%}
  {%- set run_lit = dbt_deja_vu.fingerprint_lit(invocation_id) -%}
  {%- set registration_in = dbt_deja_vu.fingerprint_registration_verdicts() -%}
  {%- set selected_in = "(" ~ ((selected_parents | join(', ')) or 'null') ~ ")" -%}
  {%- set blocking_in = "('" ~ (dbt_deja_vu.fingerprint_blocking_verdicts() | map('string') | join("', '")) ~ "')" -%}
  {#
    recorded: parents built in this run that this run recorded a verdict or skip for. A parent's verdict lost in a later
    step of the deploy leaves an earlier step's verdict counting, so the row that counts cannot show the loss.
  #}
  {%- set latest = dbt_deja_vu.fingerprint_try_query(
      "with deploy_rows as (select deploy_id, node_id, verdict, finished_at, registered_at, invocation_id from " ~ node_rel
      ~ " where deploy_id = " ~ deploy_lit ~ " and node_id in (" ~ (parents | join(', ')) ~ ")),"
      ~ " latest as (select node_id, verdict from deploy_rows" ~ dbt_deja_vu.fingerprint_latest_row() ~ ")"
      ~ " select (select count_if(verdict in " ~ blocking_in ~ ") from latest) as blocking,"
      ~ " (select count(*) from latest) as registered,"
      ~ " (select count_if(node_id in " ~ selected_in ~ ") from latest) as selected_registered,"
      ~ " (select count(distinct node_id) from deploy_rows where node_id in " ~ selected_in ~ " and invocation_id = " ~ run_lit
      ~ " and verdict not in " ~ registration_in ~ ") as selected_recorded") -%}
  {%- if not latest.ok -%}
    {{ return(dbt_deja_vu.fingerprint_guard_failed(node, "reading its parents' verdicts", latest.error)) }}
  {%- endif -%}
  {%- set res = latest.table.rows[0] -%}
  {%- set blocking_count = res[0] | int -%}
  {%- set registered = res[1] | int -%}
  {%- set unregistered = (selected_parents | length) - (res[2] | int) -%}
  {%- set unrecorded = (selected_parents | length) - (res[3] | int) -%}
  {%- if registered == 0 or blocking_count > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " parents registered=" ~ registered ~ " blocking=" ~ blocking_count ~ "; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- if unregistered > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " has " ~ unregistered ~ " parent(s) built in this run with no registration in this deploy; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- if unrecorded > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " has " ~ unrecorded ~ " parent(s) built in this run that recorded no verdict or skip in this run; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}

  {#
    The node's own SQL: its checksum has to match the one recorded at its last fingerprinted build of this
    relation. Scoped to the relation because a baseline says "this SQL last built this table"; a table the
    fingerprint has never seen (a model newly guarded, or a new alias) has to be built once before it can skip.
  #}
  {%- set current = dbt_deja_vu.fingerprint_node_checksum(node) -%}
  {%- set node_lit = dbt_deja_vu.fingerprint_lit(node.unique_id) -%}
  {%- set rel_lit = dbt_deja_vu.fingerprint_lit(dbt_deja_vu.fingerprint_relation_name(node)) -%}
  {%- set baseline = dbt_deja_vu.fingerprint_try_query(
      "select checksum, deploy_id, to_char(finished_at, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'),"
      ~ " to_char(registered_at, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'), sql_hash from " ~ node_rel
      ~ " where node_id = " ~ node_lit ~ " and relation_name = " ~ rel_lit
      ~ " and verdict not in " ~ registration_in
      ~ " order by finished_at desc nulls last, registered_at desc limit 1") -%}
  {%- if not baseline.ok -%}
    {{ return(dbt_deja_vu.fingerprint_guard_failed(node, 'reading its last build', baseline.error)) }}
  {%- endif -%}
  {%- set base = baseline.table -%}
  {%- if base.rows | length == 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " has no fingerprinted build of this relation on record; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- if current != base.rows[0][0] -%}
    {%- do log("fingerprint: " ~ node.name ~ " checksum differs from its last build in deploy " ~ base.rows[0][1] ~ "; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}

  {# What the node renders, which its checksum cannot see into. A missing hash on either side never matches. #}
  {%- set current_sql = dbt_deja_vu.fingerprint_sql_hash(node) -%}
  {%- set base_sql = base.rows[0][4] -%}
  {%- if current_sql is none -%}
    {%- do log("fingerprint: " ~ node.name ~ " has no compiled SQL to hash; building", info=true) -%}
    {{ return(false) }}
  {%- elif base_sql is none -%}
    {%- do log("fingerprint: " ~ node.name ~ " has no compiled SQL hash at its last build in deploy " ~ base.rows[0][1] ~ "; building", info=true) -%}
    {{ return(false) }}
  {%- elif current_sql != base_sql -%}
    {%- do log("fingerprint: " ~ node.name ~ " compiled SQL differs from its last build in deploy " ~ base.rows[0][1] ~ "; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}

  {#
    The ledger since the node's last build or skip (`since`). unseen: a later registration of this relation under
    another checksum with no verdict. moved, per parent and deploy on the row that counts: a verdict whose snapshot
    predates `since` (the parent was rebuilt after the node read it), or a blocking verdict or pending row in
    another deploy. This deploy's own verdicts after its snapshot are the ones checked above. unsettled: a parent
    registration since `since`, from another run, that no verdict or skip settled, which a lost verdict in a later
    step of a deploy leaves behind an earlier step's verdict that still counts.
  #}
  {%- set since = dbt_deja_vu.fingerprint_ts_lit(base.rows[0][2]) -%}
  {%- set since_registered = dbt_deja_vu.fingerprint_ts_lit(base.rows[0][3]) -%}
  {%- set since_res = dbt_deja_vu.fingerprint_try_query(
      "select (select count(*) from " ~ node_rel
      ~ " where node_id = " ~ node_lit ~ " and relation_name = " ~ rel_lit
      ~ " and registered_at > " ~ since_registered
      ~ " and checksum is distinct from " ~ dbt_deja_vu.fingerprint_lit(current) ~ ") as unseen,"
      ~ " (select count(*) from (select deploy_id, verdict, snapshot_at from " ~ node_rel
      ~ " where node_id in (" ~ (parents | join(', ')) ~ ") and verdict <> 'reselected'"
      ~ " and coalesce(finished_at, registered_at) > " ~ since
      ~ dbt_deja_vu.fingerprint_latest_row() ~ ")"
      ~ " where (deploy_id <> " ~ deploy_lit ~ " and verdict in " ~ blocking_in ~ ")"
      ~ " or (verdict not in ('pending', 'skipped') and snapshot_at < " ~ since ~ ")) as moved, "
      ~ dbt_deja_vu.fingerprint_unsettled_sql(node_rel, "r.node_id in (" ~ (parents | join(', ')) ~ ")"
          ~ " and r.registered_at > " ~ since ~ " and r.invocation_id is distinct from " ~ run_lit) ~ " as unsettled") -%}
  {%- if not since_res.ok -%}
    {{ return(dbt_deja_vu.fingerprint_guard_failed(node, 'reading the ledger since its last build', since_res.error)) }}
  {%- endif -%}
  {%- set later = since_res.table.rows[0] -%}
  {%- if later[0] | int > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " was registered under another checksum since its last build and recorded no verdict; building", info=true) -%}
    {{ return(false) }}
  {%- elif later[1] | int > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " has " ~ (later[1] | int) ~ " parent verdict(s) since its last build that this deploy's verdicts do not cover; building", info=true) -%}
    {{ return(false) }}
  {%- elif later[2] | int > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " has " ~ (later[2] | int) ~ " parent registration(s) since its last build that never recorded a verdict or skip; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}

  {%- if current is none -%}
    {%- do log("fingerprint: " ~ node.name ~ " has no checksum on either side; skipping on parent verdicts and compiled SQL", info=true) -%}
  {%- else -%}
    {%- do log("fingerprint: " ~ node.name ~ " parents registered=" ~ registered ~ " blocking=0, checksum and compiled SQL unchanged; may skip", info=true) -%}
  {%- endif -%}
  {{ return(true) }}
{% endmacro %}

{% macro fingerprint_mark_skipped(node=none, detail=none) %}
  {%- if not execute or not dbt_deja_vu.fingerprint_enabled() -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set node = node if node is not none else model -%}
  {%- set node_rel = dbt_deja_vu.fingerprint_relation('deploy_node') -%}
  {%- set deploy_id = dbt_deja_vu.fingerprint_deploy_id() -%}
  {%- set key = " where deploy_id = " ~ dbt_deja_vu.fingerprint_lit(deploy_id)
      ~ " and node_id = " ~ dbt_deja_vu.fingerprint_lit(node.unique_id) -%}
  {%- set res = dbt_deja_vu.fingerprint_try_query(
      "insert into " ~ node_rel
      ~ " (deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column, verdict, detail, registered_at, finished_at, pre_shape, checksum, sql_hash, invocation_id)"
      ~ " select deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column,"
      ~ " 'skipped', " ~ dbt_deja_vu.fingerprint_lit(detail) ~ ", registered_at, current_timestamp(), pre_shape, checksum, "
      ~ dbt_deja_vu.fingerprint_lit(dbt_deja_vu.fingerprint_sql_hash(node)) ~ ", " ~ dbt_deja_vu.fingerprint_lit(invocation_id)
      ~ " from " ~ node_rel ~ key ~ " and verdict = 'pending'") -%}
  {%- if not res.ok -%}
    {%- do log("fingerprint: WARNING: " ~ node.name ~ " was skipped but the skip could not be recorded (" ~ res.error
        ~ "); its registration in this run of deploy " ~ deploy_id ~ " stays unsettled, so its children build", info=true) -%}
  {%- endif -%}
{% endmacro %}
