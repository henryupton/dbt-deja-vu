# dbt-deja-vu

Content fingerprints for dbt deploys on Snowflake. A deploy that rebuilds every descendant of a changed model can
stop at the first table whose content came out identical: the hooks record whether each build changed its table,
and a guard lets a materialization skip a child whose parents all came out unchanged. Build systems call this early
cutoff.

Everything is a no-op until `fingerprint: true` is set. Skipping needs two more things:
`fingerprint_skip_unchanged_upstream` on, and a materialization that calls `fingerprint_should_skip()` before it
builds. The package ships no materialization, so wrapping or overriding yours is up to you (see the guard below).

## Installation

```yaml
# packages.yml
packages:
  - git: "https://github.com/henryupton/dbt-deja-vu.git"
    revision: 0.0.1
```

## Usage

```yaml
# dbt_project.yml
on-run-start:
  - "{{ dbt_deja_vu.fingerprint_register() }}"

models:
  +post-hook: "{{ dbt_deja_vu.fingerprint_post(this) }}"

seeds:
  +post-hook: "{{ dbt_deja_vu.fingerprint_post(this) }}"

snapshots:
  +post-hook: "{{ dbt_deja_vu.fingerprint_post(this) }}"
```

```bash
dbt build --vars '{"fingerprint": true, "deploy_id": "deploy-123"}'
```

Every resource type the register enrols needs the post-hook: an enrolled node whose post-hook never runs stays `pending` and blocks every child. List materializations that never run hooks (custom `streamlit`, `semantic_view` or `agent` types, say) in `fingerprint_exclude_materializations`: their nodes are not enrolled, and their children always build.

`fingerprint_register()` appends, under a building command (see `fingerprint_commands`), one `pending` row per selected model, seed and snapshot (ephemeral models excepted) to a ledger table (`fingerprint_deploy_node`), stamped with Snowflake's clock as `snapshot_at`, the node's parents, its column shape and its source checksum. `fingerprint_post(this)` then compares the table as built with the table `at(timestamp => snapshot_at)` and appends a verdict row, which also records `sql_hash`, the md5 of the compiled SQL the build ran. A ledger created by 0.0.1 gains that column at the next registration (`add column if not exists`). The ledger is append-only, so threads finishing together never queue on a table lock. Per node and deploy the latest real verdict counts; a `skipped` row stands only when the node recorded no verdict in that deploy, and `pending` only when nothing else exists. A verdict is measured against the deploy's snapshot, so it is cumulative over every step and retry of the deploy, and a later skip wrote nothing.

| Verdict | Meaning | Blocks a child's skip |
|---------|---------|-----------------------|
| `new` | Object created or replaced after the snapshot, or a view | yes |
| `unchanged` | Nothing written and the row count is equal, or the rewritten months hash equal and nothing sits above the watermark | no |
| `appended` | Rows sit above the pre-build watermark (`max(_loaded_at)`) and nothing at or below it changed: nothing was written there, or the rewritten months hash equal | no |
| `modified` | A column was added, removed or retyped (a retype only when the shape was registered at run start), a month at or below the watermark counts or hashes differently, or rows other than pure appends were written to a table with no watermark at the snapshot | yes |
| `unhashable` | No loaded-at column on one side, a loaded-at column that is not a timestamp or date, or a table without row timestamps (`ROW_TIMESTAMP` off, which includes Iceberg) | yes |
| `error` | Relation missing, no Time Travel (retention 0), a snapshot older than the table's Time Travel retention, or a fingerprint statement failed (the step and Snowflake's message are in `detail`) | yes |
| `skipped` | The guard skipped the build | no |

The comparison is cheap until it has to hash. Object state comes from `show tables like`, shape from `show columns`, and the counts are filtered scalar subqueries on `METADATA$ROW_LAST_COMMIT_TIME` and the watermark, which prune to the partitions the build wrote. Only then are the touched `_loaded_at` months hashed with `hash_agg` on both sides (every month when every row was rewritten), and per-month row counts catch a deletion in a month the build never touched. Rows with a null loaded-at value form their own bucket and count as old rows, so they cannot pass as appended. The watermark is compared in the column's own type, so `timestamp_ntz` columns are safe under any session timezone. `GEOGRAPHY` and `GEOMETRY` columns cannot be hashed and are left out; an `unchanged` or `appended` verdict names them in its `detail`. Per-month detail lands in `fingerprint_deploy_segment`.

`fingerprint_should_skip()` is for a materialization to call before building. It returns true only when all of these hold:

- `fingerprint_skip_unchanged_upstream` is on.
- At least one parent is registered in this deploy, and none is `pending`, `new`, `modified`, `unhashable` or `error`. Ephemeral parents are looked through to their own parents (an ephemeral model has no relation and no hooks, so it never carries a verdict).
- Every parent this run builds (in `selected_resources`) is registered in this deploy. One that is not was built where the ledger cannot see it, because its registration failed or the command is not in `fingerprint_commands`, so nothing vouches for it.
- Neither the node nor any parent has a materialization listed in `fingerprint_exclude_materializations`. Such a parent carries no verdict, so nothing says it did not move.
- The node's meta does not set `fingerprint_skip: false`. That opt-out is for a model whose output moves when its parents do not, such as one reading the wall clock (`current_date`); the node is still registered and fingerprinted, so its children read its verdict as usual.
- The node's own source checksum matches the one recorded at its last fingerprinted build of the same relation. A node whose own SQL changed builds however its parents came out, and a model newly guarded or under a new alias builds once first.
- The md5 of the node's compiled SQL matches the `sql_hash` on that same row. The checksum covers the model's own file; the compiled SQL also carries every macro it calls, every var it renders and every ephemeral model it reads, so a change in any of them builds it. A row with no hash (written by 0.0.1) never allows a skip, so every guarded node builds once after the upgrade. SQL that renders a run-varying value (`invocation_id`, `run_started_at`) never matches, so such a model never skips, which is the intended outcome for one that renders the clock. A model that reads the clock in SQL (`current_date()`) renders the same text every run and needs `fingerprint_skip: false`. A model whose SQL renders differently under `is_incremental()` builds once after each full refresh.
- Nothing the ledger recorded since that build (or skip) says a parent moved after the node read it. A parent's verdict is measured from its deploy's snapshot, so on its own it cannot vouch for a parent rebuilt after the node. The node builds on a parent verdict recorded since then whose snapshot predates the node's build (a later step of one deploy rebuilt the parent), on a blocking verdict or `pending` row for a parent in another deploy, and on a registration of the node itself under another source checksum that recorded no verdict (a build whose hooks failed after the table was written). Registration runs before the nodes compile, so it records the checksum alone: after a build under other compiled SQL whose hooks failed, the node builds while that SQL stands but can skip once it is reverted.

Two blind spots. Config that changes how the materialization writes without changing the SQL it renders (a `unique_key` or `incremental_strategy` set in YAML, say) leaves both the checksum and the hash equal. And a run with the fingerprint off, or one whose registration failed (see Failures), never reaches the ledger, so a parent it rebuilt without its children goes unseen by later runs. Deploy after either with `fingerprint_skip_unchanged_upstream` off.

`fingerprint_mark_skipped()` records the skip so the node's own children can read it. `integration_tests/macros/guarded_table.sql` shows the shape:

```jinja
{% set existing_relation = load_relation(this) %}
{% if existing_relation is not none and existing_relation.is_table and dbt_deja_vu.fingerprint_should_skip() %}
  {% do dbt_deja_vu.fingerprint_mark_skipped(detail='no blocking parent verdict') %}
  {% call statement('main') %}select 'skipped' as outcome{% endcall %}
  {{ return({'relations': [existing_relation]}) }}
{% endif %}
```

| Var | Default | Purpose |
|-----|---------|---------|
| `fingerprint` | `false` | Master switch; every macro is a no-op without it |
| `fingerprint_skip_unchanged_upstream` | `false` | Lets `fingerprint_should_skip()` return true |
| `fingerprint_commands` | `build`, `run`, `seed`, `snapshot`, `retry` | The commands that register, compared with `flags.WHICH`. `compile`, `docs generate`, `test` and (on Fusion) `show` fire `on-run-start` too, and a registration there would sit `pending` and block every child, so any command not listed registers nothing. A building command left off the list registers nothing either: a guarded child of a parent built in the same run builds, and later runs cannot see what it built, as with the fingerprint off. An engine that does not expose `flags.WHICH` registers anyway |
| `deploy_id` | `invocation_id` | Shared by the steps of one deploy; registration is idempotent per deploy id |
| `fingerprint_schema` | `target.schema` | Schema holding the two ledger tables; point it at a dedicated schema so model-schema housekeeping (orphan drops, schema rebuilds) never sees the ledger |
| `fingerprint_loaded_at_column` | `_loaded_at` | Watermark and month-bucket column; per model via `meta.fingerprint_loaded_at` |
| `fingerprint_exclude` | `_batched_at`, `_written_at`, `_synthesised_at` | Columns left out of the hash; per model via `meta.fingerprint_exclude` |
| `fingerprint_exclude_scd_columns` | `false` | Also leaves the scd validity columns out of every model's hash: `_is_current` and `_valid_to`, or the names dbt-scd2-utils resolves for the model when its `dbt_scd2_utils` var or the model's config sets them. Off, a new version that closes an old row reads `modified`, which is what a child joining as-of on `_valid_to` needs. On, that version reads `appended`, and a change that only moves validity (a `valid_to` fix, a `default_valid_to` change) reads `unchanged`, so children that read those columns can skip on stale validity. Turn it on only where no child reads them |
| `fingerprint_exclude_materializations` | `[]` | Materializations never fingerprinted: their nodes are not registered and their post-hook is a no-op, so a type that never runs hooks stops sitting `pending`. A child of one never skips |
| `fingerprint_select_tag` | none | Fallback selection for runtimes without `selected_resources` |
| `fingerprint_register_batch_size` | `200` | Rows per registration insert. Snowflake cannot retry a statement over 1 MB, which a single insert reaches at about 500 nodes; each row carries the node's column shape, so lower it for very wide tables. Every batch keeps the already-registered check, so registration stays idempotent per deploy id |

Requirements: dbt Fusion, or dbt-core 1.10 or later (for `config.meta_get`); only Fusion has run the integration suite. The compiled SQL is read from `model.compiled_code` and hashed with `local_md5`; an engine without either never skips. Snowflake with Time Travel on the fingerprinted tables (transient tables cap at one day, which is enough for a deploy) and row timestamps (`ROW_TIMESTAMP_DEFAULT` on the account or schema, or per-table `ROW_TIMESTAMP`); a table without them reads `unhashable` rather than failing. A build has to keep the object for the snapshot to survive: truncate + insert, `insert overwrite`, `merge` and `insert` all do; `create or replace` reads `new`, which is the right verdict for a first build or a column-set change.

## Failures

Nothing the fingerprint runs can fail a node or a run. Jinja has no try, so every statement the hooks and the guard issue goes through `fingerprint_try_query`, which runs it inside a Snowflake Scripting block (`execute immediate`) whose exception handler hands the error back instead of raising. Each caller resolves a failure in the blocking direction, never into a skip:

- `fingerprint_register()` logs a warning and stops, leaving registered whatever had committed. This matters because a failed on-run-start hook stops the run: on Fusion 2.0.0-preview.210 it ran no node at all, and `skip_nodes_if_on_run_start_fails` does the same by design. An unregistered node builds as usual, its post-hook is a no-op, and a guarded child of a parent built in the same run builds (see the guard). Later runs of the same deploy cannot see what the run built, as with the fingerprint off, so the warning says to run them with `fingerprint_skip_unchanged_upstream` off.
- `fingerprint_post(this)` records `error`, with the failed step and Snowflake's message in `detail`. When the ledger itself cannot be written, it logs a warning instead, and the node's registration stays `pending`, which blocks its children just the same.
- `fingerprint_should_skip()` logs a warning and returns false, so the node builds.
- `fingerprint_mark_skipped()` logs a warning and the skip stands. The node's row for the deploy stays `pending`, so its children build, and its own next build compares against the last row it did record.

The known cases (missing relation, no Time Travel, a snapshot older than the table's Time Travel retention, no row timestamps, unsupported column types) are still verdicts. A table created or replaced after the snapshot is caught from `created_on` before any Time Travel read, so Snowflake's `000707` cannot arise there; a `000707` from anything else (retention cut mid-deploy, say) records `error`. A fingerprint statement can still fail a node only when the session itself fails (a dropped connection), and that fails the build anyway.

The blocks cost time, not scans. Each adds about 0.35 s of Snowflake time to the statement it wraps (measured on the dev account), so statements that run back to back share a block: the post-hook issues 8 on the hashing path where it issued 10 statements before, the guard 3 and a recorded skip 1. On the integration suite that is roughly 2 s more per fingerprinted node and 1 s more per guard decision.

## Integration tests

`integration_tests/` holds a stateful scenario suite that runs against a Snowflake dev schema. See
`integration_tests/README.md`.
