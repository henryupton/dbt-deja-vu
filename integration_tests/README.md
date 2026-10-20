# Integration tests for dbt-deja-vu

A stateful scenario suite that drives the fingerprint hooks and guard through ordered deploys against a Snowflake dev
schema, and asserts the verdict every fixture records.

## Fixtures

`models/fingerprint/`, `models/fingerprint_unprotected/`, `models/fingerprint_norowts/` and `models/fingerprint_ledger/`;
the pruning group's are in `models/fingerprint_prune/` and `models/fingerprint_prune_input/` (see Pruning scenarios).

Fixtures for the content fingerprint macros (`macros/` in the package). Every
materialization shape the fingerprint has to read is here: dbt-scd2-utils' `scd` types 1 and 2 (a test-only dependency), dbt
`incremental` merge and append, dbt `table`, a test-only `overwrite_table` (insert overwrite into the
existing table), merges with `on_schema_change` set, a view, a table without a
loaded-at column, and a table kept at retention 0. Each parent has a child built by the test-only
`guarded_table` materialization, which asks `fingerprint_should_skip()` first.

- **`fp_ledger`** - view over this deploy's rows in the `fingerprint_deploy_node` ledger; the
  `matches_expected_seed` test compares it to `seeds/fingerprint/fp_expected_<n>.csv`, keyed by
  the `fp_iteration` var. Enabled only with the `fingerprint` var on, as are the three singular tests
  below: the ledger table exists only once a fingerprint run has created it, so a plain `dbt build`
  on a fresh schema leaves all four out.
- **`fp_child_late`** - disabled unless `fp_enable_late_child` is set, so it can appear mid-sequence.

The `fp_edp` group (scenarios 33 to 38) holds common warehouse shapes:

- **`fp_dim_versioned`** - a versioned `scd` dim built as `fp_dim_versioned_v1`; `fp_child_of_versioned`
  refs the bare name, so its `depends_on` carries the versioned unique_id and the latest-version pointer
  view Fusion may create is never fingerprinted.
- **`fp_stg_batch`** - a staging merge whose `_loaded_at` is a batch stamp, steered by `fp_batch_stage`:
  empty at snapshot, then two batches, then a late chunk of the current batch landing exactly at the
  watermark (must read `modified`), then a new batch.
- **`fp_registry`** - a registry seed with no loaded-at column. It reads
  `unhashable` whether or not its content changed, so `fp_child_of_registry` always builds. Scenario 37
  pins that.
- **`fp_seed_live`** - a seed with `_loaded_at` whose CSV the runner rewrites in scenario 37 so a reload
  carries changed content and reads `modified`. The runner restores the committed CSVs when it exits.

The guard also refuses to skip a node whose own source checksum, or whose compiled SQL hash, differs
from its last fingerprinted build of the same relation. Fixture model SQL is steered by vars, not edits,
and no guarded fixture renders a var that moves, so guarded checksums and hashes never move between
scenarios except where a scenario means them to: the runner rewrites two seed CSVs, one guarded model
(`fp_edge_child_multi`, in scenarios 46 and 54) and two macros a guarded model renders (`fp_sql_expr`, from
scenario 66, and `fp_prune_as_of_join`, from 93) and restores them when it exits; a variant a killed run left behind is recognised at start
and the committed copy restored from git. Every group's first scenario builds each guarded child, so the
expected verdicts hold from the first run on a fresh schema and on every rerun.

The `fp_edge` group (scenarios 39 to 57) holds the guard's remaining branches and the hooks' edge cases:

- **`fp_edge_parent_a`**, **`fp_edge_parent_b`** - two merge parents with a value flip each; `b` can also be
  made to fail, so it stays `pending` for the rest of its deploy.
- **`fp_edge_eph`** and **`fp_edge_child_of_eph`** - an ephemeral over `a` and a guarded child that reads only
  through it. The ephemeral is never registered; the guard looks through it to `a`.
- **`fp_edge_child_multi`** and **`fp_edge_grandchild`** - a two-parent child (one blocking parent builds it,
  as does its own SQL changing) and a guarded child of it, which skips only when its parent was skipped.
- **`fp_edge_adopted`** - a table first built with the fingerprint off, under an alias fresh per run: no
  baseline for the relation, so it builds once, then skips.
- **`fp_edge_scd2`** - the validity blind spot pinned: a full refresh with a different `default_valid_to` reads
  `unchanged` with `fingerprint_exclude_scd_columns` set true (scenario 48) and `modified` with it false.
  Scenario 4 sets it true too, so a later version of an old key reads `appended` on `fp_dim_scd2`.
- **`fp_edge_norowts`** (its folder switches `ROW_TIMESTAMP` off before the fingerprint runs) and
  **`fp_edge_numeric`** (an epoch-number loaded-at) - both `unhashable`, both children always build.
- **`fp_edge_geo`** - a `GEOGRAPHY` column left out of the hash beside an `OBJECT` and an `ARRAY` that are in it.
- **`fp_edge_quoted`** - mixed-case, space-bearing identifiers, `meta.fingerprint_loaded_at` and a per-model
  `meta.fingerprint_exclude` on a column that changes every build.
- **`fp_edge_nulls`** - rows with a null loaded-at, moved into the null bucket, added, changed and removed;
  none may read `appended`.
- **`fp_edge_ntz`** and **`fp_edge_date`** - `timestamp_ntz` under a Pacific/Auckland session and `date`
  loaded-at columns: a row one hour above the watermark reads `appended`, a row on the watermark day `modified`.

Three singular tests ride along with every group: `fp_one_pending_per_node` (registration stays idempotent
across the steps and retries of one deploy), `fp_skipped_left_alone` (no row of a skipped node's table
was committed at or after the deploy's snapshot) and `fp_register_batched` (no register statement wrote more
than `fingerprint_register_batch_size` rows; one statement stamps all its rows with one `registered_at`). The fault group adds a fourth, `fp_fault_error_recorded`. Scenarios 52 and 53 fail a parent on purpose: the
children then build on its `pending` verdict, and a retry under the same deploy id settles it against the
original snapshot. Scenarios 54 and 55 decide `fp_edge_child_multi` twice in one deploy:

- **54** - built in one step (its own SQL changed) and skipped in the next; its `new` verdict still counts
  over the skip, so `fp_edge_grandchild` builds.
- **55** - skipped in one step and rebuilt in the next, where a full refresh replaces `fp_edge_parent_a`
  with the same rows (`new`, blocking); the rebuild records `new`, so `fp_edge_grandchild` builds. The
  parent's other child, `fp_edge_child_of_eph`, rebuilds in the same step: a child left behind a blocking
  parent verdict from another deploy builds at its next selection, which would break a rerun from 46.

Scenarios 56 and 57 replace `fp_edge_parent_a` in a deploy that selects none of its guarded children, then
run the whole group: the parent reads `unchanged` there, and its children still build on the other
deploy's `new`, which arrived after their last build.

The `fp_knob` group (scenarios 58 to 63) holds the vars a project sets to fit the fingerprint to its own
deploy:

- **`fp_knob_parent`** and **`fp_knob_child`** - a merge parent that stays unchanged after the group's
  initial, and its guarded child, the control that skips.
- **`fp_knob_hookless`** and **`fp_knob_child_of_hookless`** - a table built by the test-only `hookless_table`
  materialization, which runs no hooks (the serving-layer shape), and a guarded child that also reads
  `fp_knob_parent`. The group lists `hookless_table` in `fingerprint_exclude_materializations`, so the table is
  never registered and its child always builds.
- **`fp_knob_wallclock`** - a guarded child of `fp_knob_parent` that reads the wall clock. Its meta
  `fingerprint_skip` comes from the `fp_knob_wallclock_skip` var, so the opt-out flips without moving the
  checksum.
- **`fp_knob_scd2`** and **`fp_knob_child_of_scd2`** - an scd type 2 dim on its own source var
  (`fp_knob_scd_source`) and a guarded child that reads `_valid_from` and `_valid_to`, as an as-of join does.
- **59** - a `dbt compile --write-catalog` with the fingerprint on, under the build's deploy id, registers
  nothing (`fingerprint_commands`); the build step after it registers only what it selects.
- **60** - nothing changed: the control and the wall-clock child skip, and the child of the hookless table
  builds on that parent alone.
- **61** - the same again with `fingerprint_skip: false` on the wall-clock child, which now builds.
- **62** - a new version of an old key closes its old row. Under the default (`fingerprint_exclude_scd_columns`
  false) the dim reads `modified` and its guarded child builds.
- **63** - `fingerprint_register_batch_size` 1, over two steps of one deploy: every register statement carries
  one row, and the second step's batches for the first step's nodes insert a `reselected` row, never a second
  `pending`.

The `fp_sql` group (scenarios 64 onwards) holds what moves a guarded child's output without moving its
checksum:

- **`fp_sql_parent`** - a merge parent that stays unchanged after the group's initial. Its pre-hook
  (`fp_sql_legacy_row_hook`) can append a hashless copy of a child's latest build row, as 0.0.1 wrote it.
- **`fp_sql_child`** - the control, a guarded child that renders nothing that moves.
- **`fp_sql_child_of_macro`** - a guarded child that renders `fp_sql_status_expr` (`macros/fp_sql_expr.sql`).
- **`fp_sql_child_of_var`** - a guarded child that renders the `fp_sql_label` var into its SQL.
- **65** - nothing changed: every child skips, so the post-hook and the guard hash the same text.
- **66** - the runner rewrites the macro (`upper` to `lower`): only its child builds.
- **67** - `fp_sql_label` changes: only its child builds.
- **68** - a hashless row planted as the control's latest build: the control builds.
- **69** - nothing changed again: every child skips on the hashes recorded since.

The `fp_fault` group (scenarios 70 to 77) holds statements that fail inside the fingerprint, which must never fail
a node or turn into a skip; a failed registration instead stops the run unless it is contained. The faults are real
Snowflake errors, induced without any switch in the package:

- **`fp_fault_parent`** and **`fp_fault_child`** - a merge parent that stays unchanged after the group's initial,
  and its guarded child, the control that skips whenever the ledger works.
- **`fp_fault_parked`** - a merge child of `fp_fault_parent`, so its post-hook always hashes. With
  `fp_fault_park_segment` set, its pre-hook renames the segment ledger table away and its own post-hook, which
  runs after `fingerprint_post`, renames it back (`macros/fp_fault_park_segment.sql`).
- **`fp_fault_child_of_parked`** - a guarded child of both.
- **71** - `fingerprint_schema` is `INFORMATION_SCHEMA`, where no role can create a table: registration fails at
  its first statement, `fingerprint_contain_register_failure` contains it, and every post-hook and guard then reads
  a ledger that is not there. Every node builds and succeeds, and each post-hook warns that its registration could
  not be read either, so the build may be invisible to the ledger; the next step, on the real ledger, skips as before.
- **72** - the segment table goes missing while `fp_fault_parked`'s post-hook hashes: it records `error` with the
  step and Snowflake's message (`fp_fault_error_recorded` checks the detail), and its guarded child builds.
- **73** - a run whose registration wrote nothing (a `dbt run` left off `fingerprint_commands`, the state a
  contained registration failure leaves) rebuilds `fp_fault_parked`; its guarded child builds in that run, though
  its other parent is registered and unchanged.
- **74** - nothing changed: the segment table is back and every guarded child skips.
- **75** - the same unreachable ledger under the default, with a change to `fp_fault_parent`: the run fails before
  the parent builds.
- **76** - a new deploy builds the change, which reads `modified`, and `fp_fault_child` builds. Had 75 built the
  parent, this deploy would read it `unchanged` against its own snapshot and skip the child on stale content.
- **77** - back to the steady state.

The `fp_settle` group (scenarios 78 to 86) loses a verdict on purpose: a run that registered a node and then
recorded nothing for it may have written its table, so that registration must keep blocking until a verdict or skip
settles it. The fixtures:

- **`fp_settle_parent`** - a merge parent; `fp_settle_flip` lowers a status below the watermark.
- **`fp_settle_child`** - its guarded child, which also renders `fp_settle_label` into its SQL.
- Either loses its verdict when `fp_settle_lose` names it: its pre-hook renames the node ledger away and its own
  post-hook, after `fingerprint_post`, renames it back (`macros/fp_settle_lose_verdict.sql`). A parked ledger a
  killed run left behind stops the build rather than being dropped.

The scenarios:

- **79** - the parent reads `unchanged` in step A, then in step B is rebuilt with a change and loses its verdict. Step
  A's verdict still counts for the deploy, so only the missing record from this run builds the child.
- **80** - the same loss in a deploy that selects no child.
- **81** - the next deploy reads the parent `unchanged` against its own snapshot; 80's unsettled registration builds
  the child.
- **82** - nothing changed: the child, rebuilt since, skips.
- **83** - `fp_settle_label` changes, so the child builds under other compiled SQL, and loses its own verdict.
- **84** - the label reverted, the child's SQL and checksum match its last recorded build again, and its parent
  reads `unchanged`; 83's unsettled registration of the child builds it.
- **85** - the same within one deploy: step A's unsettled registration builds the child in step B.
- **86** - nothing changed: the child skips.

The `fp_mask` group (scenarios 87 to 90) holds a run-start audit stamp rendered into compiled SQL:

- **`fp_mask_parent`** - a merge parent that stays unchanged after the group's initial.
- **`fp_mask_child_of_stamp`** - a guarded child that renders `run_started_at` as a `_batched_at` stamp, the shape of
  an audit-columns macro.
- **`fp_mask_child_of_clock`** - a guarded child that renders it in the same format in a filter.
- **88** - unmasked, both children build: their compiled SQL moves with the run start.
- **89** - `fingerprint_sql_mask_run_started_at` names the stamp's template: the stamped child still builds once,
  against a baseline hashed unmasked.
- **90** - masked again: the stamped child skips; the child reading the clock in logic builds.

When the ledger is unreachable nothing is recorded, so a step can also assert on its own output: `expect_log` lists
patterns that must appear (the warnings, the guard's reason for building) and `forbid_log` patterns that must not
(`guarded_table: skipping`).

The source rows come from `fp_customer_rows()` (`macros/fp_fixture_sql.sql`), steered by vars:
`fp_source` picks a seed (a fixture may pass its own), `fp_upper_email` / `fp_null_email_customer` change values below the
watermark, `fp_extra_column` / `fp_drop_column` change the shape, `fp_duplicate_rows`,
`fp_bump_loaded_at` and `fp_shuffle` exercise the hash.

## Pruning scenarios

The `fp_prune` group (scenarios 91 to 103) shows end to end which nodes the guard prunes, and why, on fixtures shaped
like the EDP trial line (support-agent adherence). Every node is `fp_prune_<name>`:

```
stg_reports (frozen) -> evt_report (incremental, delete+insert) -> int_adh --\
stg_agent_lifecycle (frozen) -> dim_agent (scd 2) ---------------------------+-> fct_daily (left as-of join) -> dm_fct
                                dim_agent -> dm_dim_agent (copies the validity columns)
                                dim_agent -> dim_agent_current (view, _is_current) -> rpt
stg_country_lifecycle (frozen) -> dim_country (scd 2) -> dm_country -> dashboard (hookless, excluded)
                                  dim_country -> striking (reads current_date(), fingerprint_skip false)
```

- Every guarded node is built by the test-only `guarded_overwrite` (`macros/guarded_overwrite.sql`), EDP's
  `truncate_insert` with the guard in it. It keeps the object under `--full-refresh` (insert overwrite by column name,
  shared with `overwrite_table`) and replaces it only when it is missing, is not a table, or `force_full_refresh` is
  set. Hooks, `persist_docs`, the commit and `apply_grants` run in `truncate_insert`'s order. The guard sits after the
  replace decision and before the first pre-hook, so a pruned node runs none of them: its grants, persisted docs and
  whatever its post-hooks apply (EDP's sensitivity tags) stay as its last build left them.
- On Fusion the view, the built-in incremental and dbt-scd2-utils' `scd` run only inside-transaction hooks, so
  `fingerprint_post` has to be a plain post-hook. As `transaction: false` it would never run on those nodes, and they
  would stay `pending` and block every child.
- The scd dims (dbt-scd2-utils) and the built-in incremental event have no guard, so they always build; their verdicts
  are what the guarded nodes read.
- `fct_daily` is the only model that renders `fp_prune_as_of_join` (`macros/fp_prune_as_of_join.sql`), so rewriting it
  is a macro-only change. It is EDP's `as_of_join` shape: a left join on a timestamp cursor (`occurred_at`, cast from
  the report date as EDP's event casts it), falling back to an agent's first version (`_change_type = 'I'`) for a
  report dated before it. A report whose agent has no version yet keeps a null team, as EDP's fact keeps a null
  `support_agent_key`.
- Every fixture renders EDP's audit columns (`fp_prune_audit_columns()` in `macros/fp_prune.sql`: a `_batched_at`
  run-start literal and a `_written_at` write clock), and the group sets `fingerprint_sql_mask_run_started_at` to the
  stamp's template. Scenario 103 drops the mask.
- The frozen inputs (`models/fingerprint_prune_input/`) are tables in a shared schema that no deploy selects. The runner
  rebuilds them with the fingerprint off whenever a scenario's `stage` differs from the last, from seeds under
  `seeds/fingerprint_prune/` whose rows land by stage.

Each scenario (`arms=True` in the runner) builds its selection twice from the same inputs, each as its own deploy:

- **control** - guard off, in `<target schema>_prune_control`, with its ledger there.
- **treatment** - guard on (off in 91, 98 and 101), in `<target schema>_prune_treatment`, with its own ledger.

The ledgers have to be separate: the guard reads every deploy's rows for a parent's node id, and both arms share node
ids, so one ledger would let the control's verdicts block the treatment's skips. Scenario 100 is where that shows: the
control's `fct_daily` reads `modified` there while the treatment's is pruned, and on one ledger that verdict builds the
treatment's `dm_fct`. `macros/fp_prune.sql` routes the arm models by the `fp_prune_arm` var (`generate_schema_name`),
and `fp_prune_setup` creates the three schemas with one day of Time Travel, which every table inherits.

That retention is a precondition of every pruned cell, not a fixture detail. At retention 0 every fingerprinted table
reads `error`, which blocks, so nothing prunes and the guard looks broken. EDP's `ANALYTICS` schemas keep 28 days
(transient tables cap at one, enough for a deploy), but its dev schemas inherit `DEV_ANALYTICS`' 0. A trial there
needs `data_retention_time_in_days = 1` set on its schemas first, or has to run in prod-like schemas.

Each arm's verdicts are asserted against `fp_expected_<n>_control.csv` and `fp_expected_<n>_treatment.csv`, every
registered node listed, `skipped` meaning pruned. A third step then tests `fp_prune_arms_agree`
(`tests/fingerprint_prune/`, enabled only by `fp_prune_oracle`) under the treatment's vars, even when the treatment's
build failed, so neither check hides the other. The step must log the test's pass: a test that is disabled or selects
nothing exits 0. It is a content oracle that does not use the package's hash: `count(*)` and `hash_agg` of every
fixture, `_batched_at` and `_written_at` left out, must match across the arms. Scenarios 97, 99 and 100 declare their
staleness (`stale` in the runner, per relation): the treatment must equal its own content at the deploy's snapshot;
the control must hold exactly `behind` rows above the treatment's watermark and, at or below it, as many rows as the
treatment, exactly `changed` of them different on each side.

Every scenario but 102 selects the whole line (`--select tag:fp_prune`), as a trial harness does, so the matrix
shows what the guard does when every node is selected. That is not EDP's deploy selector. EDP deploys run
`dbt build --select state:modified.body+ --full-refresh`, which picks only nodes whose own file changed, and their
descendants:

- A macro-only change (93) selects nothing, so it never reaches the warehouse in a deploy.
- A data-only change (92, 94 to 101) is never picked up by a state selector, only by a run that selects the line.
- A change on the agent line never selects the country branch, so `dm_country` is not pruned there, just not built.
- Parents a deploy does not select are not registered in it. 102 runs EDP's selector against a manifest parsed before
  a body-only change to `int_adh`, and shows the guard pruning on the one parent that is.

The matrix is the treatment arm, generated from those seeds by `./test_fingerprint_sequence.py --write-matrix`
(`--check-matrix` fails when it is out of date, and so does any run that includes a prune scenario, before dbt
starts). A pruned cell gives the verdict the control's build recorded; `not selected` marks a node the scenario's
selector left out.

<!-- fp_prune matrix: generated by ./test_fingerprint_sequence.py --write-matrix; do not edit by hand -->
| Node | 91 S1 baseline (guard off) | 92 S2 no-op | 93 S3 macro only | 94 S4 backdated version | 95 S5 forward-dated version | 96 S6 daily (no --full-refresh) | 97 S7 appended | 98 S8 resync (guard off) | 99 late report | 100 late agent | 101 resync again (guard off) | 102 state:modified.body+ deploy | 103 mask off |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `fp_prune_evt_report` | built: new | built: new | built: new | built: new | built: new | built: unchanged | built: new | built: new | built: new | built: new | built: new | not selected | built: new |
| `fp_prune_int_adh` | built: new | built: unchanged | built: unchanged | built: unchanged | built: unchanged | **pruned** (control: unchanged) | built: appended | built: unchanged | built: appended | built: unchanged | built: unchanged | built: unchanged | built: unchanged |
| `fp_prune_dim_agent` | built: new | built: unchanged | built: unchanged | built: modified | built: modified | built: modified | built: appended | built: unchanged | built: modified | built: appended | built: unchanged | not selected | built: unchanged |
| `fp_prune_fct_daily` | built: new | **pruned** (control: unchanged) | built: unchanged | built: modified | built: unchanged | built: unchanged | **pruned** (control: appended; stale, 4 row(s) behind) | built: appended | built: appended | **pruned** (control: modified; stale, 1 row(s) changed) | built: modified | **pruned** (control: unchanged) | built: unchanged |
| `fp_prune_dm_fct` | built: new | **pruned** (control: unchanged) | **pruned** (control: unchanged) | built: modified | **pruned** (control: unchanged) | **pruned** (control: unchanged) | **pruned** (control: appended; stale, 4 row(s) behind) | built: appended | **pruned** (control: appended; stale, 1 row(s) behind) | **pruned** (control: modified; stale, 1 row(s) behind) | built: appended | **pruned** (control: unchanged) | built: unchanged |
| `fp_prune_dm_dim_agent` | built: new | **pruned** (control: unchanged) | **pruned** (control: unchanged) | built: modified | built: modified | built: modified | **pruned** (control: appended; stale, 1 row(s) behind) | built: appended | built: modified | **pruned** (control: appended; stale, 1 row(s) behind) | built: appended | not selected | built: unchanged |
| `fp_prune_dim_agent_current` | built: new | built: new | built: new | built: new | built: new | built: new | built: new | built: new | built: new | built: new | built: new | not selected | built: new |
| `fp_prune_rpt` | built: new | built: unchanged | built: unchanged | built: unchanged | built: modified | built: modified | built: appended | built: unchanged | built: modified | built: appended | built: unchanged | not selected | built: unchanged |
| `fp_prune_dim_country` | built: new | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | not selected | built: unchanged |
| `fp_prune_dm_country` | built: new | **pruned** (control: unchanged) | **pruned** (control: unchanged) | **pruned** (control: unchanged) | **pruned** (control: unchanged) | **pruned** (control: unchanged) | **pruned** (control: unchanged) | built: unchanged | **pruned** (control: unchanged) | **pruned** (control: unchanged) | built: unchanged | not selected | built: unchanged |
| `fp_prune_dashboard` | built, not registered | built, not registered | built, not registered | built, not registered | built, not registered | built, not registered | built, not registered | built, not registered | built, not registered | built, not registered | built, not registered | not selected | built, not registered |
| `fp_prune_striking` | built: new | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | built: unchanged | not selected | built: unchanged |
<!-- fp_prune matrix end -->

What each column shows, and the rule that prunes or blocks:

- **91 S1 baseline** - guard off in both arms and every object created or replaced (`force_full_refresh`, and the scd
  `full_refresh_strategy` replace), so every node reads `new` and every guarded node has a baseline on record: its
  checksum and its masked compiled SQL hash.
- **92 S2 no-op** - the whole line built with nothing changed, under `--full-refresh` as EDP deploys run. The built-in
  incremental replaces the event (`new`), which blocks `int_adh`; it rebuilds `unchanged`. Every guarded node whose
  parents all read `unchanged` prunes: `fct_daily`, `dm_fct`, `dm_dim_agent` and `dm_country`. `rpt` builds because its
  parent is a view, which always reads `new`; `striking` because it opts out; the dashboard because it is never
  registered.
- **93 S3 macro only** - the as-of macro is rewritten with the same predicates the other way round. `fct_daily`'s
  checksum holds but its compiled SQL moves, so it builds; it reads `unchanged`, so `dm_fct` prunes behind it (early
  cutoff).
- **94 S4 backdated version** - an agent version dated between two existing ones lands. The dim reads `modified` (an
  old row's `_valid_to` moved), so `fct_daily` rebuilds and reads `modified` too (an old report now resolves to the new
  version), as do `dm_fct` and `dm_dim_agent`. `rpt` rebuilds `unchanged`: no current version moved.
- **95 S5 forward-dated version** - a version dated after every report closes a current row. The dim and
  `dm_dim_agent` read `modified`; `fct_daily` rebuilds, but no report resolves differently, so it reads `unchanged`
  and `dm_fct` prunes.
- **96 S6 daily** - another forward-dated version, deployed without `--full-refresh`. The incremental event has nothing
  new to write and reads `unchanged`, so `int_adh` prunes as well. `fct_daily` still builds: one blocking parent (the
  dim) is enough, and a pruned one (`int_adh`) never blocks.
- **97 S7 appended** - a new agent and a day of reports land. The dim and `int_adh` read `appended`, which never
  blocks, so `fct_daily`, `dm_fct` and `dm_dim_agent` prune on content that lacks the new rows. That is the guard's
  logic-only propagation: stale by design, declared to the oracle as 4, 4 and 1 rows behind. Here the reports and
  their agent land together, so the staleness is only missing rows; 100 shows it changing rows.
- **98 S8 resync** - the treatment rebuilt with the guard off. The stale nodes read `appended`, and the oracle finds the
  arms identical again.
- **99 late report** - a report lands for an agent with no version yet, with a forward-dated version for another agent.
  The dim reads `modified`, so `fct_daily` builds and carries the report with a null team (`appended`); `dm_fct`
  prunes on that appended verdict, one row behind.
- **100 late agent** - the agent's first version lands, dated after the report, so only the fallback resolves it. The
  dim reads `appended`, so `fct_daily` prunes and keeps the null team, a changed row at its watermark that the control
  reads `modified`, not a missing one. `dm_fct` and `dm_dim_agent` prune one row behind. Under the left join, appended
  never blocking means stale content, not only missing rows.
- **101 resync again** - the guard off in the treatment once more: `fct_daily` reads `modified` (the team filled in),
  `dm_fct` and `dm_dim_agent` `appended`.
- **102 state:modified.body+ deploy** - a comment line added to `int_adh`, deployed under EDP's selector against the
  manifest the runner parsed before writing it. Only `int_adh`, `fct_daily` and `dm_fct` are selected. `int_adh`
  builds, because its one parent, the event, is not registered in the deploy, and reads `unchanged`.
  `fct_daily` has one registered parent of two, `unchanged`, and nothing in the ledger since its last build says the
  unselected dim moved, so it prunes, and `dm_fct` behind it.
- **103 mask off** - the no-op deploy again without `fingerprint_sql_mask_run_started_at`. Every guarded node renders
  the run-start stamp, so every compiled SQL hash moves and nothing prunes. It runs last because the unmasked hashes it
  records would build every guarded node once more at the next masked deploy.

## Running

```bash
dbt deps
```

```bash
# 103 ordered, stateful scenarios; each builds a selection with fingerprint: true (bar
# scenario 50's first step, built with it off on purpose, and scenario 59's, a compile) and
# asserts every fixture's verdict (new / unchanged / appended / modified / unhashable / error /
# skipped) against fp_expected_<n>. Weighted towards false negatives: genuine changes that must
# not be waved through, and guarded children that must build.
./test_fingerprint_sequence.py --profile default --target dev

# A slice, with the ledger dumped after each scenario
./test_fingerprint_sequence.py --only 9,10,12 --show

# Scenario 12 deletes fixture rows inside the build with a pre-hook (fp_delete_customer var).

# The pruning group alone: each scenario rebuilds the frozen inputs if its stage moved, then builds
# the control arm and the treatment arm
./test_fingerprint_sequence.py --from 91

# Regenerate the pruning matrix from the expected seeds, or fail when it is out of date (no dbt)
./test_fingerprint_sequence.py --write-matrix
./test_fingerprint_sequence.py --check-matrix
```

Fixture tables and the seeds under `seeds/fingerprint` get `data_retention_time_in_days = 1` from a
post-hook, because the fingerprint reads the pre-build table through Time Travel and a dev database may
have retention 0. The pruning group's schemas get it from `fp_prune_setup`, which the runner calls first, so
every table in them inherits it.
