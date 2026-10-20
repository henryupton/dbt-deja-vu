{{
    config(
        enabled=(var('fingerprint', false) | string | lower) in ['true', '1'] and (var('fp_prune_oracle', false) | string | lower) in ['true', '1'],
        tags=['fp_prune']
    )
}}

{#
  The content oracle, independent of the package's hash. The runner tests it on its own (fp_prune_oracle) once both
  arms have built the same selection from the same frozen inputs, with the treatment's vars, so a failure here never
  skips the ledger checks of the build. Compares count(*) and hash_agg of every fixture across the two schemas, audit
  stamps left out. A relation fp_prune_stale names (relation: rows) is stale on purpose: the treatment must equal its
  own content at its deploy's snapshot, and the control that content plus exactly that many rows, every one above the
  treatment's watermark. One row per relation that disagrees.
#}
{%- set stale = var('fp_prune_stale', {}) or {} -%}
{%- set checks = [] -%}
{%- if execute -%}
  {%- set snap = none -%}
  {%- if stale | length > 0 -%}
    {%- set res = run_query("select to_char(min(snapshot_at), 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM') from "
        ~ dbt_deja_vu.fingerprint_relation('deploy_node') ~ " where deploy_id = "
        ~ dbt_deja_vu.fingerprint_lit(var('deploy_id', invocation_id)) ~ " and verdict = 'pending'") -%}
    {%- set snap = dbt_deja_vu.fingerprint_ts_lit(res.rows[0][0]) -%}
  {%- endif -%}
  {%- set fixtures = [] -%}
  {%- for n in graph.nodes.values() -%}
    {%- if n.resource_type == 'model' and 'fp_prune' in (n.tags or []) and n.name != 'fp_ledger' -%}
      {%- do fixtures.append(n.name) -%}
      {%- set control = api.Relation.create(database=target.database, schema=fp_prune_schema('control'), identifier=n.alias) -%}
      {%- set treatment = api.Relation.create(database=target.database, schema=fp_prune_schema('treatment'), identifier=n.alias) -%}
      {%- set cols = [] -%}
      {%- for c in adapter.get_columns_in_relation(control) -%}
        {%- if (c.name | upper) not in ['_BATCHED_AT', '_WRITTEN_AT'] -%}{%- do cols.append(adapter.quote(c.name)) -%}{%- endif -%}
      {%- endfor -%}
      {%- set agg = "count(*) as n, hash_agg(" ~ (cols | join(', ')) ~ ") as h" -%}
      {%- if n.name in stale -%}
        {%- do checks.append(
            "select '" ~ n.name ~ "' as relation_name, 'stale by " ~ stale[n.name] ~ " row(s)' as expectation,"
            ~ " c.n as control_rows, t.n as treatment_rows"
            ~ " from (select " ~ agg ~ " from " ~ control ~ ") c,"
            ~ " (select " ~ agg ~ " from " ~ treatment ~ ") t,"
            ~ " (select " ~ agg ~ " from " ~ treatment ~ " at(timestamp => " ~ snap ~ ")) tp,"
            ~ " (select " ~ agg ~ " from " ~ control ~ " where _loaded_at <= (select max(_loaded_at) from " ~ treatment ~ ")) co"
            ~ " where t.n <> tp.n or not equal_null(t.h, tp.h)"
            ~ " or co.n <> t.n or not equal_null(co.h, t.h)"
            ~ " or c.n - t.n <> " ~ (stale[n.name] | int)) -%}
      {%- else -%}
        {%- do checks.append(
            "select '" ~ n.name ~ "' as relation_name, 'equal' as expectation, c.n as control_rows, t.n as treatment_rows"
            ~ " from (select " ~ agg ~ " from " ~ control ~ ") c, (select " ~ agg ~ " from " ~ treatment ~ ") t"
            ~ " where c.n <> t.n or not equal_null(c.h, t.h)") -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if fixtures | length == 0 -%}
    {%- do exceptions.raise_compiler_error("fp_prune_arms_agree found no fp_prune fixture to compare") -%}
  {%- endif -%}
  {%- for name in stale -%}
    {%- if name not in fixtures -%}
      {%- do exceptions.raise_compiler_error("fp_prune_stale names " ~ name ~ ", which is not an fp_prune fixture") -%}
    {%- endif -%}
  {%- endfor -%}
{%- endif -%}
{%- if checks | length == 0 %}
select null as relation_name, null as expectation, null as control_rows, null as treatment_rows where false
{%- else %}
{{ checks | join('\nunion all\n') }}
{%- endif %}
