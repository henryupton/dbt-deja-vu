# Integration tests for dbt-deja-vu

A stateful scenario suite that drives the fingerprint hooks and guard through ordered deploys against a Snowflake dev
schema, and asserts the verdict every fixture records.

## Fixtures

`models/fingerprint/`, `models/fingerprint_unprotected/`, `models/fingerprint_norowts/` and `models/fingerprint_ledger/`.

Fixtures for the content fingerprint macros (`macros/` in the package). Every
materialization shape the fingerprint has to read is here: dbt-scd2-utils' `scd` types 1 and 2 (a test-only dependency), dbt
`incremental` merge and append, dbt `table`, a test-only `overwrite_table` (insert overwrite into the
existing table), merges with `on_schema_change` set, a view, a table without a
loaded-at column, and a table kept at retention 0. Each parent has a child built by the test-only
`guarded_table` materialization, which asks `fingerprint_should_skip()` first.

- **`fp_ledger`** - view over this deploy's rows in the `fingerprint_deploy_node` ledger; the
  `matches_expected_seed` test compares it to `seeds/fingerprint/fp_expected_<n>.csv`, keyed by
  the `fp_iteration` var. Enabled only with the `fingerprint` var on, as are the two singular tests
  below: the ledger table exists only once a fingerprint run has created it, so a plain `dbt build`
  on a fresh schema leaves all three out.
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

The guard also refuses to skip a node whose own source checksum differs from its last fingerprinted
build of the same relation. Fixture model SQL is steered by vars, not edits, so guarded checksums never
move between scenarios except where a scenario means them to: the runner rewrites two seed CSVs and one
guarded model (`fp_edge_child_multi`, in scenarios 46 and 54) and restores them when it exits; a variant
a killed run left behind is recognised at start and the committed copy restored from git. Every group's
first scenario builds each guarded child, so the expected verdicts hold from the first run on a fresh
schema and on every rerun.

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

Two singular tests ride along with every group: `fp_one_pending_per_node` (registration stays idempotent
across the steps and retries of one deploy) and `fp_skipped_left_alone` (no row of a skipped node's table
was committed at or after the deploy's snapshot). Scenarios 52 and 53 fail a parent on purpose: the
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

The `fp_knob` group (scenarios 58 onwards) holds the vars a project sets to fit the fingerprint to its own
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

The source rows come from `fp_customer_rows()` (`macros/fp_fixture_sql.sql`), steered by vars:
`fp_source` picks a seed (a fixture may pass its own), `fp_upper_email` / `fp_null_email_customer` change values below the
watermark, `fp_extra_column` / `fp_drop_column` change the shape, `fp_duplicate_rows`,
`fp_bump_loaded_at` and `fp_shuffle` exercise the hash.

## Running

```bash
dbt deps
```

```bash
# Sixty-two ordered, stateful scenarios; each builds a selection with fingerprint: true (bar
# scenario 50's first step, built with it off on purpose, and scenario 59's, a compile) and
# asserts every fixture's verdict (new / unchanged / appended / modified / unhashable / error /
# skipped) against fp_expected_<n>. Weighted towards false negatives: genuine changes that must
# not be waved through, and guarded children that must build.
./test_fingerprint_sequence.py --profile default --target dev

# A slice, with the ledger dumped after each scenario
./test_fingerprint_sequence.py --only 9,10,12 --show

# Scenario 12 deletes fixture rows inside the build with a pre-hook (fp_delete_customer var).
```

Fixture tables and the seeds under `seeds/fingerprint` get `data_retention_time_in_days = 1` from a
post-hook, because the fingerprint reads the pre-build table through Time Travel and a dev database may
have retention 0.
